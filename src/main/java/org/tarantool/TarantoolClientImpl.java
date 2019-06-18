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
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
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
    protected long operationTimeout;

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
                reconnect(thumbstone);
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
        initClient(socketProvider, config);
        if (socketProvider instanceof ConfigurableSocketChannelProvider) {
            ConfigurableSocketChannelProvider configurableProvider = (ConfigurableSocketChannelProvider) socketProvider;
            configurableProvider.setConnectionTimeout(config.connectionTimeout);
            configurableProvider.setRetriesLimit(config.retryCount);
        }
        startConnector(config.initTimeoutMillis);
    }

    private void initClient(SocketChannelProvider socketProvider, TarantoolClientConfig config) {
        this.thumbstone = NOT_INIT_EXCEPTION;
        this.config = config;
        this.initialRequestSize = config.defaultRequestSize;
        this.operationTimeout = config.operationExpiryTimeMillis;
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
        if (!config.useNewCall) {
            setCallCode(Code.OLD_CALL);
            this.syncOps.setCallCode(Code.OLD_CALL);
            this.fireAndForgetOps.setCallCode(Code.OLD_CALL);
            this.composableAsyncOps.setCallCode(Code.OLD_CALL);
        }
    }

    private void startConnector(long initTimeoutMillis) {
        connector.start();
        try {
            if (!waitAlive(initTimeoutMillis, TimeUnit.MILLISECONDS)) {
                CommunicationException e = new CommunicationException(
                    initTimeoutMillis +
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

    protected void reconnect(Throwable lastError) {
        SocketChannel channel = null;
        int retryNumber = 0;
        while (!Thread.currentThread().isInterrupted()) {
            try {
                channel = socketProvider.get(retryNumber++, lastError == NOT_INIT_EXCEPTION ? null : lastError);
            } catch (Exception e) {
                closeChannel(channel);
                lastError = e;
                if (!(e instanceof SocketProviderTransientException)) {
                    close(e);
                    return;
                }
            }
            try {
                if (channel != null) {
                    connect(channel);
                    return;
                }
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
            TarantoolGreeting greeting = ProtoUtils.connect(channel, config.username, config.password, msgPackLite);
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

    /**
     * Executes an operation with default timeout.
     *
     * @param code operation code
     * @param args operation arguments
     *
     * @return deferred result
     *
     * @see #setOperationTimeout(long)
     */
    protected Future<?> exec(Code code, Object... args) {
        return doExec(operationTimeout, code, args);
    }

    /**
     * Executes an operation with the given timeout.
     * {@code timeoutMillis} will override the default
     * timeout. 0 means the limitless operation.
     *
     * @param code operation code
     * @param args operation arguments
     *
     * @return deferred result
     */
    protected Future<?> exec(long timeoutMillis, Code code, Object... args) {
        return doExec(timeoutMillis, code, args);
    }

    protected TarantoolOp<?> doExec(long timeoutMillis, Code code, Object[] args) {
        validateArgs(args);
        long sid = syncId.incrementAndGet();

        TarantoolOp<?> future = makeNewOperation(timeoutMillis, sid, code, args);

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

    protected TarantoolOp<?> makeNewOperation(long timeoutMillis, long sid, Code code, Object[] args) {
        return new TarantoolOp<>(sid, code, args)
            .orTimeout(timeoutMillis, TimeUnit.MILLISECONDS);
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
        ByteBuffer buffer = ProtoUtils.createPacket(msgPackLite, code, syncId, schemaId, args);

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
                TarantoolPacket packet = ProtoUtils.readPacket(readChannel, msgPackLite);

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

    protected void fail(TarantoolOp<?> future, Exception e) {
        future.completeExceptionally(e);
    }

    protected void complete(TarantoolPacket packet, TarantoolOp<?> future) {
        if (future != null) {
            long code = packet.getCode();
            if (code == 0) {
                if (future.getCode() == Code.EXECUTE) {
                    completeSql(future, packet);
                } else {
                    ((TarantoolOp) future).complete(packet.getBody().get(Key.DATA.getId()));
                }
            } else {
                Object error = packet.getBody().get(Key.ERROR.getId());
                fail(future, serverError(code, error));
            }
        }
    }

    protected void completeSql(TarantoolOp<?> future, TarantoolPacket pack) {
        Long rowCount = SqlProtoUtils.getSqlRowCount(pack);
        if (rowCount != null) {
            ((TarantoolOp) future).complete(rowCount);
        } else {
            List<Map<String, Object>> values = SqlProtoUtils.readSqlResult(pack);
            ((TarantoolOp) future).complete(values);
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

    /**
     * Gets the default timeout for client operations.
     *
     * @return timeout in millis
     */
    public long getOperationTimeout() {
        return operationTimeout;
    }

    /**
     * Sets the default operation timeout.
     *
     * @param operationTimeout timeout in millis
     */
    public void setOperationTimeout(long operationTimeout) {
        this.operationTimeout = operationTimeout;
    }

    @Override
    public boolean isAlive() {
        return state.getState() == StateHelper.ALIVE && thumbstone == null;
    }

    @Override
    public boolean isClosed() {
        return state.getState() == StateHelper.CLOSED;
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

    protected boolean isDead(TarantoolOp<?> future) {
        if (this.thumbstone != null) {
            fail(future, new CommunicationException("Connection is dead", thumbstone));
            return true;
        }
        return false;
    }

    protected static class TarantoolOp<V> extends CompletableFuture<V> {

        /**
         * A task identifier used in {@link TarantoolClientImpl#futures}.
         */
        private final long id;

        /**
         * Tarantool binary protocol operation code.
         */
        private final Code code;

        /**
         * Arguments of operation.
         */
        private final Object[] args;

        public TarantoolOp(long id, Code code, Object[] args) {
            this.id = id;
            this.code = code;
            this.args = args;
        }

        public long getId() {
            return id;
        }

        public Code getCode() {
            return code;
        }

        public Object[] getArgs() {
            return args;
        }

        /**
         * Missed in jdk8 CompletableFuture operator to limit execution
         * by time.
         */
        public TarantoolOp<V> orTimeout(long timeout, TimeUnit unit) {
            if (timeout < 0) {
                throw new IllegalArgumentException("Timeout cannot be negative");
            }
            if (unit == null) {
                throw new IllegalArgumentException("Time unit cannot be null");
            }
            if (timeout == 0 || isDone()) {
                return this;
            }
            ScheduledFuture<?> abandonByTimeoutAction = TimeoutScheduler.EXECUTOR.schedule(
                () -> {
                    if (!this.isDone()) {
                        this.completeExceptionally(new TimeoutException());
                    }
                },
                timeout, unit
            );
            whenComplete(
                (ignored, error) -> {
                    if (error == null && !abandonByTimeoutAction.isDone()) {
                        abandonByTimeoutAction.cancel(false);
                    }
                }
            );
            return this;
        }

        /**
         * Runs timeout operation as a delayed task.
         */
        static class TimeoutScheduler {

            static final ScheduledThreadPoolExecutor EXECUTOR;

            static {
                EXECUTOR =
                    new ScheduledThreadPoolExecutor(1, new TarantoolThreadDaemonFactory("tarantoolTimeout"));
                EXECUTOR.setRemoveOnCancelPolicy(true);
            }
        }

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

    protected class ComposableAsyncOps
        extends AbstractTarantoolOps<Integer, List<?>, Object, CompletionStage<List<?>>> {

        @Override
        public CompletionStage<List<?>> exec(Code code, Object... args) {
            return (CompletionStage<List<?>>) TarantoolClientImpl.this.exec(code, args);
        }

        @Override
        public void close() {
            TarantoolClientImpl.this.close();
        }

    }

}
