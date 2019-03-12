package org.tarantool;

import org.tarantool.protocol.ProtoUtils;
import org.tarantool.protocol.ReadableViaSelectorChannel;
import org.tarantool.protocol.TarantoolGreeting;
import org.tarantool.protocol.TarantoolPacket;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

public class TarantoolClientImpl extends TarantoolBase<Future<?>> implements TarantoolClient {

    public static final CommunicationException NOT_INIT_EXCEPTION
        = new CommunicationException("Not connected, initializing connection");

    protected TarantoolClientConfig config;

    /**
     * External.
     */
    protected SocketChannelProvider socketProvider;
    protected SocketChannel channel;
    protected ReadableViaSelectorChannel readChannel;

    protected volatile Exception thumbstone;

    protected Map<Long, TarantoolOp<?>> futures;
    protected AtomicInteger pendingResponsesCount = new AtomicInteger();

    /**
     * Write properties.
     */
    protected ByteBuffer sharedBuffer;
    protected ReentrantLock bufferLock = new ReentrantLock(false);
    protected Condition bufferNotEmpty = bufferLock.newCondition();
    protected Condition bufferEmpty = bufferLock.newCondition();

    protected ByteBuffer writerBuffer;
    protected ReentrantLock writeLock = new ReentrantLock(true);

    /**
     * Interfaces.
     */
    protected SyncOps syncOps;
    protected FireAndForgetOps fireAndForgetOps;
    protected ComposableAsyncOps composableAsyncOps;

    /**
     * Inner.
     */
    protected TarantoolClientStats stats;
    protected StateHelper state = new StateHelper(StateHelper.RECONNECT);
    protected Thread reader;
    protected Thread writer;

    protected Thread connector = new Thread(new Runnable() {
        @Override
        public void run() {
            while (!Thread.currentThread().isInterrupted()) {
                reconnect(0, thumbstone);
                try {
                    state.awaitReconnection();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        }
    });

    public TarantoolClientImpl(String address, TarantoolClientConfig config) {
        this(new SingleSocketChannelProviderImpl(address), config);
    }

    public TarantoolClientImpl(SocketChannelProvider socketProvider, TarantoolClientConfig config) {
        super();
        this.thumbstone = NOT_INIT_EXCEPTION;
        this.config = config;
        this.initialRequestSize = config.defaultRequestSize;
        this.socketProvider = socketProvider;
        this.stats = new TarantoolClientStats();
        this.futures = new ConcurrentHashMap<>(config.predictedFutures);
        this.sharedBuffer = ByteBuffer.allocateDirect(config.sharedBufferSize);
        this.writerBuffer = ByteBuffer.allocateDirect(sharedBuffer.capacity());
        this.connector.setDaemon(true);
        this.connector.setName("Tarantool connector");
        this.syncOps = new SyncOps();
        this.composableAsyncOps = new ComposableAsyncOps();
        this.fireAndForgetOps = new FireAndForgetOps();
        if (config.useNewCall) {
            setCallCode(Code.CALL);
            this.syncOps.setCallCode(Code.CALL);
            this.fireAndForgetOps.setCallCode(Code.CALL);
            this.composableAsyncOps.setCallCode(Code.CALL);
        }

        startConnector(config);
    }

    private void startConnector(TarantoolClientConfig config) {
        connector.start();
        try {
            if (!waitAlive(config.initTimeoutMillis, TimeUnit.MILLISECONDS)) {
                CommunicationException e = new CommunicationException(
                    config.initTimeoutMillis +
                        "ms is exceeded when waiting for client initialization. " +
                        "You could configure init timeout in TarantoolConfig"
                );

                close(e);
                throw e;
            }
        } catch (InterruptedException e) {
            close(e);
            throw new IllegalStateException(e);
        }
    }

    protected void reconnect(int retry, Throwable lastError) {
        SocketChannel channel;
        while (!Thread.currentThread().isInterrupted()) {
            try {
                channel = socketProvider.get(retry++, lastError == NOT_INIT_EXCEPTION ? null : lastError);
            } catch (Exception e) {
                close(e);
                return;
            }
            try {
                connect(channel);
                return;
            } catch (Exception e) {
                closeChannel(channel);
                lastError = e;
                if (e instanceof InterruptedException) {
                    Thread.currentThread().interrupt();
                }
            }
        }
    }

    protected void connect(final SocketChannel channel) throws Exception {
        try {
            TarantoolGreeting greeting = ProtoUtils.connect(channel, config.username, config.password);
            this.serverVersion = greeting.getServerVersion();
        } catch (IOException e) {
            closeChannel(channel);
            throw new CommunicationException("Couldn't connect to tarantool", e);
        }

        channel.configureBlocking(false);
        this.channel = channel;
        this.readChannel = new ReadableViaSelectorChannel(channel);

        bufferLock.lock();
        try {
            sharedBuffer.clear();
        } finally {
            bufferLock.unlock();
        }
        this.thumbstone = null;
        startThreads(channel.socket().getRemoteSocketAddress().toString());
    }

    protected void startThreads(String threadName) throws InterruptedException {
        final CountDownLatch ioThreadStarted = new CountDownLatch(2);
        final AtomicInteger leftIoThreads = new AtomicInteger(2);
        reader = new Thread(() -> {
            ioThreadStarted.countDown();
            if (state.acquire(StateHelper.READING)) {
                try {
                    readThread();
                } finally {
                    state.release(StateHelper.READING);
                    // only last of two IO-threads can signal for reconnection
                    if (leftIoThreads.decrementAndGet() == 0) {
                        state.trySignalForReconnection();
                    }
                }
            }
        });
        writer = new Thread(() -> {
            ioThreadStarted.countDown();
            if (state.acquire(StateHelper.WRITING)) {
                try {
                    writeThread();
                } finally {
                    state.release(StateHelper.WRITING);
                    // only last of two IO-threads can signal for reconnection
                    if (leftIoThreads.decrementAndGet() == 0) {
                        state.trySignalForReconnection();
                    }
                }
            }
        });
        state.release(StateHelper.RECONNECT);

        configureThreads(threadName);
        reader.start();
        writer.start();
        ioThreadStarted.await();
    }

    protected void configureThreads(String threadName) {
        reader.setName("Tarantool " + threadName + " reader");
        writer.setName("Tarantool " + threadName + " writer");
        writer.setPriority(config.writerThreadPriority);
        reader.setPriority(config.readerThreadPriority);
    }

    protected Future<?> exec(Code code, Object... args) {
        return doExec(code, args);
    }

    protected CompletableFuture<?> doExec(Code code, Object[] args) {
        validateArgs(args);
        long sid = syncId.incrementAndGet();
        TarantoolOp<?> future = new TarantoolOp<>(code);

        if (isDead(future)) {
            return future;
        }
        futures.put(sid, future);
        if (isDead(future)) {
            futures.remove(sid);
            return future;
        }
        try {
            write(code, sid, null, args);
        } catch (Exception e) {
            futures.remove(sid);
            fail(future, e);
        }
        return future;
    }

    protected synchronized void die(String message, Exception cause) {
        if (thumbstone != null) {
            return;
        }
        final CommunicationException error = new CommunicationException(message, cause);
        this.thumbstone = error;
        while (!futures.isEmpty()) {
            Iterator<Map.Entry<Long, TarantoolOp<?>>> iterator = futures.entrySet().iterator();
            while (iterator.hasNext()) {
                Map.Entry<Long, TarantoolOp<?>> elem = iterator.next();
                if (elem != null) {
                    TarantoolOp<?> future = elem.getValue();
                    fail(future, error);
                }
                iterator.remove();
            }
        }
        pendingResponsesCount.set(0);

        bufferLock.lock();
        try {
            sharedBuffer.clear();
            bufferEmpty.signalAll();
        } finally {
            bufferLock.unlock();
        }
        stopIO();
    }

    public void ping() {
        syncGet(exec(Code.PING));
    }

    protected void write(Code code, Long syncId, Long schemaId, Object... args)
        throws Exception {
        ByteBuffer buffer = ProtoUtils.createPacket(code, syncId, schemaId, args);

        if (directWrite(buffer)) {
            return;
        }
        sharedWrite(buffer);

    }

    protected void sharedWrite(ByteBuffer buffer) throws InterruptedException, TimeoutException {
        long start = System.currentTimeMillis();
        if (bufferLock.tryLock(config.writeTimeoutMillis, TimeUnit.MILLISECONDS)) {
            try {
                int rem = buffer.remaining();
                stats.sharedMaxPacketSize = Math.max(stats.sharedMaxPacketSize, rem);
                if (rem > initialRequestSize) {
                    stats.sharedPacketSizeGrowth++;
                }
                while (sharedBuffer.remaining() < buffer.limit()) {
                    stats.sharedEmptyAwait++;
                    long remaining = config.writeTimeoutMillis - (System.currentTimeMillis() - start);
                    try {
                        if (remaining < 1 || !bufferEmpty.await(remaining, TimeUnit.MILLISECONDS)) {
                            stats.sharedEmptyAwaitTimeouts++;
                            throw new TimeoutException(
                                config.writeTimeoutMillis +
                                    "ms is exceeded while waiting for empty buffer. " +
                                    "You could configure write timeout it in TarantoolConfig"
                            );
                        }
                    } catch (InterruptedException e) {
                        throw new CommunicationException("Interrupted", e);
                    }
                }
                sharedBuffer.put(buffer);
                pendingResponsesCount.incrementAndGet();
                bufferNotEmpty.signalAll();
                stats.buffered++;
            } finally {
                bufferLock.unlock();
            }
        } else {
            stats.sharedWriteLockTimeouts++;
            throw new TimeoutException(
                config.writeTimeoutMillis +
                    "ms is exceeded while waiting for shared buffer lock. " +
                    "You could configure write timeout in TarantoolConfig"
            );
        }
    }

    private boolean directWrite(ByteBuffer buffer) throws InterruptedException, IOException, TimeoutException {
        if (sharedBuffer.capacity() * config.directWriteFactor <= buffer.limit()) {
            if (writeLock.tryLock(config.writeTimeoutMillis, TimeUnit.MILLISECONDS)) {
                try {
                    int rem = buffer.remaining();
                    stats.directMaxPacketSize = Math.max(stats.directMaxPacketSize, rem);
                    if (rem > initialRequestSize) {
                        stats.directPacketSizeGrowth++;
                    }
                    writeFully(channel, buffer);
                    stats.directWrite++;
                    pendingResponsesCount.incrementAndGet();
                } finally {
                    writeLock.unlock();
                }
                return true;
            } else {
                stats.directWriteLockTimeouts++;
                throw new TimeoutException(
                    config.writeTimeoutMillis +
                        "ms is exceeded while waiting for channel lock. " +
                        "You could configure write timeout in TarantoolConfig"
                );
            }
        }
        return false;
    }

    protected void readThread() {
        while (!Thread.currentThread().isInterrupted()) {
            try {
                TarantoolPacket packet = ProtoUtils.readPacket(readChannel);

                Map<Integer, Object> headers = packet.getHeaders();

                Long syncId = (Long) headers.get(Key.SYNC.getId());
                TarantoolOp<?> future = futures.remove(syncId);
                stats.received++;
                pendingResponsesCount.decrementAndGet();
                complete(packet, future);
            } catch (Exception e) {
                die("Cant read answer", e);
                return;
            }
        }
    }

    protected void writeThread() {
        writerBuffer.clear();
        while (!Thread.currentThread().isInterrupted()) {
            try {
                bufferLock.lock();
                try {
                    while (sharedBuffer.position() == 0) {
                        bufferNotEmpty.await();
                    }
                    sharedBuffer.flip();
                    writerBuffer.put(sharedBuffer);
                    sharedBuffer.clear();
                    bufferEmpty.signalAll();
                } finally {
                    bufferLock.unlock();
                }
                writerBuffer.flip();
                writeLock.lock();
                try {
                    writeFully(channel, writerBuffer);
                } finally {
                    writeLock.unlock();
                }
                writerBuffer.clear();
                stats.sharedWrites++;
            } catch (Exception e) {
                die("Cant write bytes", e);
                return;
            }
        }
    }

    protected void fail(CompletableFuture<?> q, Exception e) {
        q.completeExceptionally(e);
    }

    protected void complete(TarantoolPacket packet, TarantoolOp<?> future) {
        if (future != null) {
            long code = packet.getCode();
            if (code == 0) {
                if (future.getCode() == Code.EXECUTE) {
                    completeSql(future, packet);
                } else {
                    ((CompletableFuture) future).complete(packet.getBody().get(Key.DATA.getId()));
                }
            } else {
                Object error = packet.getBody().get(Key.ERROR.getId());
                fail(future, serverError(code, error));
            }
        }
    }

    protected void completeSql(CompletableFuture<?> future, TarantoolPacket pack) {
        Long rowCount = SqlProtoUtils.getSqlRowCount(pack);
        if (rowCount != null) {
            ((CompletableFuture) future).complete(rowCount);
        } else {
            List<Map<String, Object>> values = SqlProtoUtils.readSqlResult(pack);
            ((CompletableFuture) future).complete(values);
        }
    }

    protected <T> T syncGet(Future<T> result) {
        try {
            return result.get();
        } catch (ExecutionException e) {
            if (e.getCause() instanceof CommunicationException) {
                throw (CommunicationException) e.getCause();
            } else if (e.getCause() instanceof TarantoolException) {
                throw (TarantoolException) e.getCause();
            } else {
                throw new IllegalStateException(e.getCause());
            }
        } catch (InterruptedException e) {
            throw new IllegalStateException(e);
        }
    }

    protected void writeFully(SocketChannel channel, ByteBuffer buffer) throws IOException {
        ProtoUtils.writeFully(channel, buffer);
    }

    @Override
    public void close() {
        close(new Exception("Connection is closed."));
        try {
            state.awaitState(StateHelper.CLOSED);
        } catch (InterruptedException ignored) {
            Thread.currentThread().interrupt();
        }
    }

    protected void close(Exception e) {
        if (state.close()) {
            connector.interrupt();
            die(e.getMessage(), e);
        }
    }

    protected void stopIO() {
        if (reader != null) {
            reader.interrupt();
        }
        if (writer != null) {
            writer.interrupt();
        }
        if (readChannel != null) {
            try {
                readChannel.close(); // also closes this.channel
            } catch (IOException ignored) {
                // no-op
            }
        }
        closeChannel(channel);
    }

    @Override
    public boolean isAlive() {
        return state.getState() == StateHelper.ALIVE && thumbstone == null;
    }

    @Override
    public void waitAlive() throws InterruptedException {
        state.awaitState(StateHelper.ALIVE);
    }

    @Override
    public boolean waitAlive(long timeout, TimeUnit unit) throws InterruptedException {
        return state.awaitState(StateHelper.ALIVE, timeout, unit);
    }

    @Override
    public TarantoolClientOps<Integer, List<?>, Object, List<?>> syncOps() {
        return syncOps;
    }

    @Override
    public TarantoolClientOps<Integer, List<?>, Object, Future<List<?>>> asyncOps() {
        return (TarantoolClientOps) this;
    }

    @Override
    public TarantoolClientOps<Integer, List<?>, Object, CompletionStage<List<?>>> composableAsyncOps() {
        return composableAsyncOps;
    }

    @Override
    public TarantoolClientOps<Integer, List<?>, Object, Long> fireAndForgetOps() {
        return fireAndForgetOps;
    }


    @Override
    public TarantoolSQLOps<Object, Long, List<Map<String, Object>>> sqlSyncOps() {
        return new TarantoolSQLOps<Object, Long, List<Map<String, Object>>>() {

            @Override
            public Long update(String sql, Object... bind) {
                return (Long) syncGet(exec(Code.EXECUTE, Key.SQL_TEXT, sql, Key.SQL_BIND, bind));
            }

            @Override
            public List<Map<String, Object>> query(String sql, Object... bind) {
                return (List<Map<String, Object>>) syncGet(exec(Code.EXECUTE, Key.SQL_TEXT, sql, Key.SQL_BIND, bind));
            }
        };
    }

    @Override
    public TarantoolSQLOps<Object, Future<Long>, Future<List<Map<String, Object>>>> sqlAsyncOps() {
        return new TarantoolSQLOps<Object, Future<Long>, Future<List<Map<String, Object>>>>() {
            @Override
            public Future<Long> update(String sql, Object... bind) {
                return (Future<Long>) exec(Code.EXECUTE, Key.SQL_TEXT, sql, Key.SQL_BIND, bind);
            }

            @Override
            public Future<List<Map<String, Object>>> query(String sql, Object... bind) {
                return (Future<List<Map<String, Object>>>) exec(Code.EXECUTE, Key.SQL_TEXT, sql, Key.SQL_BIND, bind);
            }
        };
    }

    protected class SyncOps extends AbstractTarantoolOps<Integer, List<?>, Object, List<?>> {

        @Override
        public List exec(Code code, Object... args) {
            return (List) syncGet(TarantoolClientImpl.this.exec(code, args));
        }

        @Override
        public void close() {
            throw new IllegalStateException("You should close TarantoolClient instead.");
        }

    }

    protected class FireAndForgetOps extends AbstractTarantoolOps<Integer, List<?>, Object, Long> {

        @Override
        public Long exec(Code code, Object... args) {
            if (thumbstone == null) {
                try {
                    long syncId = TarantoolClientImpl.this.syncId.incrementAndGet();
                    write(code, syncId, null, args);
                    return syncId;
                } catch (Exception e) {
                    throw new CommunicationException("Execute failed", e);
                }
            } else {
                throw new CommunicationException("Connection is not alive", thumbstone);
            }
        }

        @Override
        public void close() {
            throw new IllegalStateException("You should close TarantoolClient instead.");
        }

    }

    protected class ComposableAsyncOps
        extends AbstractTarantoolOps<Integer, List<?>, Object, CompletionStage<List<?>>> {

        @Override
        public CompletionStage<List<?>> exec(Code code, Object... args) {
            return (CompletionStage<List<?>>) TarantoolClientImpl.this.doExec(code, args);
        }

        @Override
        public void close() {
            TarantoolClientImpl.this.close();
        }

    }

    protected boolean isDead(CompletableFuture<?> q) {
        if (this.thumbstone != null) {
            fail(q, new CommunicationException("Connection is dead", thumbstone));
            return true;
        }
        return false;
    }

    /**
     * A subclass may use this as a trigger to start retries.
     * This method is called when state becomes ALIVE.
     */
    protected void onReconnect() {
        // No-op, override.
    }

    public Exception getThumbstone() {
        return thumbstone;
    }

    public TarantoolClientStats getStats() {
        return stats;
    }

    /**
     * Manages state changes.
     */
    protected final class StateHelper {

        static final int UNINITIALIZED = 0;
        static final int READING = 1;
        static final int WRITING = 2;
        static final int ALIVE = READING | WRITING;
        static final int RECONNECT = 4;
        static final int CLOSED = 8;

        private final AtomicInteger state;

        private final AtomicReference<CountDownLatch> nextAliveLatch =
            new AtomicReference<>(new CountDownLatch(1));

        private final CountDownLatch closedLatch = new CountDownLatch(1);

        /**
         * The condition variable to signal a reconnection is needed from reader /
         * writer threads and waiting for that signal from the reconnection thread.
         * <p>
         * The lock variable to access this condition.
         *
         * @see #awaitReconnection()
         * @see #trySignalForReconnection()
         */
        protected final ReentrantLock connectorLock = new ReentrantLock();
        protected final Condition reconnectRequired = connectorLock.newCondition();

        protected StateHelper(int state) {
            this.state = new AtomicInteger(state);
        }

        protected int getState() {
            return state.get();
        }

        /**
         * Set CLOSED state, drop RECONNECT state.
         */
        protected boolean close() {
            for (; ; ) {
                int currentState = getState();

                /* CLOSED is the terminal state. */
                if ((currentState & CLOSED) == CLOSED) {
                    return false;
                }

                /*  Drop RECONNECT, set CLOSED. */
                if (compareAndSet(currentState, (currentState & ~RECONNECT) | CLOSED)) {
                    return true;
                }
            }
        }

        /**
         * Move from a current state to a give one.
         * <p>
         * Some moves are forbidden.
         */
        protected boolean acquire(int mask) {
            for (; ; ) {
                int currentState = getState();

                /* CLOSED is the terminal state. */
                if ((currentState & CLOSED) == CLOSED) {
                    return false;
                }

                /* Don't move to READING, WRITING or ALIVE from RECONNECT. */
                if ((currentState & RECONNECT) > mask) {
                    return false;
                }

                /* Cannot move from a state to the same state. */
                if ((currentState & mask) != 0) {
                    throw new IllegalStateException("State is already " + mask);
                }

                /* Set acquired state. */
                if (compareAndSet(currentState, currentState | mask)) {
                    return true;
                }
            }
        }

        protected void release(int mask) {
            for (; ; ) {
                int currentState = getState();
                if (compareAndSet(currentState, currentState & ~mask)) {
                    return;
                }
            }
        }

        protected boolean compareAndSet(int expect, int update) {
            if (!state.compareAndSet(expect, update)) {
                return false;
            }

            if (update == ALIVE) {
                CountDownLatch latch = nextAliveLatch.getAndSet(new CountDownLatch(1));
                latch.countDown();
                onReconnect();
            } else if (update == CLOSED) {
                closedLatch.countDown();
            }
            return true;
        }

        /**
         * Reconnection uses another way to await state via receiving a signal
         * instead of latches.
         */
        protected void awaitState(int state) throws InterruptedException {
            if (state == RECONNECT) {
                awaitReconnection();
            } else {
                CountDownLatch latch = getStateLatch(state);
                if (latch != null) {
                    latch.await();
                }
            }
        }

        protected boolean awaitState(int state, long timeout, TimeUnit timeUnit) throws InterruptedException {
            CountDownLatch latch = getStateLatch(state);
            return (latch == null) || latch.await(timeout, timeUnit);
        }

        private CountDownLatch getStateLatch(int state) {
            if (state == CLOSED) {
                return closedLatch;
            }
            if (state == ALIVE) {
                if (getState() == CLOSED) {
                    throw new IllegalStateException("State is CLOSED.");
                }
                CountDownLatch latch = nextAliveLatch.get();
                /* It may happen so that an error is detected but the state is still alive.
                 Wait for the 'next' alive state in such cases. */
                return (getState() == ALIVE && thumbstone == null) ? null : latch;
            }
            return null;
        }

        /**
         * Blocks until a reconnection signal will be received.
         *
         * @see #trySignalForReconnection()
         */
        private void awaitReconnection() throws InterruptedException {
            connectorLock.lock();
            try {
                while (getState() != StateHelper.RECONNECT) {
                    reconnectRequired.await();
                }
            } finally {
                connectorLock.unlock();
            }
        }

        /**
         * Signals to the connector that reconnection process can be performed.
         *
         * @see #awaitReconnection()
         */
        private void trySignalForReconnection() {
            if (compareAndSet(StateHelper.UNINITIALIZED, StateHelper.RECONNECT)) {
                connectorLock.lock();
                try {
                    reconnectRequired.signal();
                } finally {
                    connectorLock.unlock();
                }
            }
        }

    }

    protected static class TarantoolOp<V> extends CompletableFuture<V> {

        /**
         * Tarantool binary protocol operation code.
         */
        private final Code code;

        public TarantoolOp(Code code) {
            this.code = code;
        }

        public Code getCode() {
            return code;
        }
    }

}
