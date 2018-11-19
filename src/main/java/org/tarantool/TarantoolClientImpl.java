package org.tarantool;

import java.io.DataInputStream;
import java.io.IOException;
import java.net.SocketException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.LockSupport;
import java.util.concurrent.locks.ReentrantLock;


public class TarantoolClientImpl extends TarantoolBase<Future<List<?>>> implements TarantoolClient {
    public static final CommunicationException NOT_INIT_EXCEPTION = new CommunicationException("Not connected, initializing connection");
    protected TarantoolClientConfig config;

    /**
     * External
     */
    protected SocketChannelProvider socketProvider;
    protected volatile Exception thumbstone;
    protected volatile CountDownLatch alive;

    protected Map<Long, FutureImpl<List<?>>> futures;
    protected AtomicInteger wait = new AtomicInteger();
    /**
     * Write properties
     */
    protected SocketChannel channel;
    protected ByteBuffer sharedBuffer;
    protected ByteBuffer writerBuffer;
    protected ReentrantLock bufferLock = new ReentrantLock(false);
    protected Condition bufferNotEmpty = bufferLock.newCondition();
    protected Condition bufferEmpty = bufferLock.newCondition();
    protected ReentrantLock writeLock = new ReentrantLock(true);

    /**
     * Interfaces
     */
    protected SyncOps syncOps;
    protected FireAndForgetOps fireAndForgetOps;

    /**
     * Inner
     */
    protected TarantoolClientStats stats;
    protected CountDownLatch stopIO;
    protected Thread reader;
    protected Thread writer;


    protected Thread connector = new Thread(new Runnable() {
        @Override
        public void run() {
            while (!Thread.currentThread().isInterrupted()) {
                reconnect(0, thumbstone);
                LockSupport.park();
            }
        }
    });

    public TarantoolClientImpl(SocketChannelProvider socketProvider, TarantoolClientConfig config) {
        super();
        this.thumbstone = NOT_INIT_EXCEPTION;
        this.config = config;
        this.initialRequestSize = config.defaultRequestSize;
        this.alive = new CountDownLatch(1);
        this.socketProvider = socketProvider;
        this.stats = new TarantoolClientStats();
        this.futures = new ConcurrentHashMap<Long, FutureImpl<List<?>>>(config.predictedFutures);
        this.sharedBuffer = ByteBuffer.allocateDirect(config.sharedBufferSize);
        this.writerBuffer = ByteBuffer.allocateDirect(sharedBuffer.capacity());
        this.connector.setDaemon(true);
        this.connector.setName("Tarantool connector");
        this.syncOps = new SyncOps();
        this.fireAndForgetOps = new FireAndForgetOps();
        if (config.useNewCall) {
            setCallCode(Code.CALL);
            this.syncOps.setCallCode(Code.CALL);
            this.fireAndForgetOps.setCallCode(Code.CALL);
        }
        connector.start();
        try {
            if (!waitAlive(config.initTimeoutMillis, TimeUnit.MILLISECONDS)) {
                 close();
                throw new CommunicationException(config.initTimeoutMillis+"ms is exceeded when waiting for client initialization. You could configure init timeout in TarantoolConfig");
            }
        } catch (InterruptedException e) {
            close();
            throw new IllegalStateException(e);
        }
    }

    protected void reconnect(int retry, Throwable lastError) {
        SocketChannel channel;
        while (!Thread.interrupted()) {
            channel = socketProvider.get(retry++, lastError == NOT_INIT_EXCEPTION ? null : lastError);
            try {
                connect(channel);
                return;
            } catch (Exception e) {
                closeChannel(channel);
                lastError = e;
            }
        }
    }

    protected void connect(final SocketChannel channel) throws Exception {
        try {
            DataInputStream is = new DataInputStream(cis = new ByteBufferInputStream(channel));
            byte[] bytes = new byte[64];
            is.readFully(bytes);
            String firstLine = new String(bytes);
            if (!firstLine.startsWith("Tarantool")) {
                close();
                throw new CommunicationException("Welcome message should starts with tarantool but starts with '" + firstLine + "'", new IllegalStateException("Invalid welcome packet"));
            }
            is.readFully(bytes);
            this.salt = new String(bytes);
            if (config.username != null && config.password != null) {
                writeFully(channel, createAuthPacket(config.username, config.password));
                readPacket(is);
                Long code = (Long) headers.get(Key.CODE.getId());
                if (code != 0) {
                    throw serverError(code, body.get(Key.ERROR.getId()));
                }
            }
            this.is = is;
        } catch (IOException e) {
            try {
                is.close();
            } catch (IOException ignored) {

            }
            try {
                cis.close();
            } catch (IOException ignored) {

            }
            throw new CommunicationException("Couldn't connect to tarantool", e);
        }
        channel.configureBlocking(false);
        this.channel = channel;
        bufferLock.lock();
        try {
            sharedBuffer.clear();
        } finally {
            bufferLock.unlock();
        }
        startThreads(channel.socket().getRemoteSocketAddress().toString());
        this.thumbstone = null;
        alive.countDown();
    }

    protected void startThreads(String threadName) throws IOException, InterruptedException {
        final CountDownLatch init = new CountDownLatch(2);
        stopIO = new CountDownLatch(2);
        reader = new Thread(new Runnable() {
            @Override
            public void run() {
                init.countDown();
                readThread();
                stopIO.countDown();
            }
        });
        writer = new Thread(new Runnable() {
            @Override
            public void run() {
                init.countDown();
                writeThread();
                stopIO.countDown();
            }
        });


        configureThreads(threadName);
        reader.start();
        writer.start();
        init.await();
    }

    protected void configureThreads(String threadName) {
        reader.setName("Tarantool " + threadName + " reader");
        writer.setName("Tarantool " + threadName + " writer");
        writer.setPriority(config.writerThreadPriority);
        reader.setPriority(config.readerThreadPriority);
    }


    public Future<List<?>> exec(Code code, Object... args) {
        validateArgs(args);
        FutureImpl<List<?>> q = new FutureImpl<List<?>>(syncId.incrementAndGet());
        if (isDead(q)) {
            return q;
        }
        futures.put(q.getId(), q);
        if (isDead(q)) {
            futures.remove(q.getId());
            return q;
        }
        try {
            write(code, q.getId(), null, args);
        } catch (Exception e) {
            futures.remove(q.getId());
            fail(q, e);
        }
        return q;
    }


    protected synchronized void die(String message, Exception cause) {
        if (thumbstone != null) {
            return;
        }
        this.thumbstone = new CommunicationException(message, cause);
        this.alive = new CountDownLatch(1);
        while (!futures.isEmpty()) {
            Iterator<Map.Entry<Long, FutureImpl<List<?>>>> iterator = futures.entrySet().iterator();
            while (iterator.hasNext()) {
                Map.Entry<Long, FutureImpl<List<?>>> elem = iterator.next();
                if (elem != null) {
                    FutureImpl<List<?>> future = elem.getValue();
                    fail(future, cause);
                }
                iterator.remove();
            }
        }

        bufferLock.lock();
        try {
            sharedBuffer.clear();
            bufferEmpty.signalAll();
        } finally {
            bufferLock.unlock();
        }
        stopIO();
        if (connector.getState() == Thread.State.WAITING) {
            LockSupport.unpark(connector);
        }
    }


    public void ping() {
        syncGet(exec(Code.PING));
    }


    protected void write(Code code, Long syncId, Long schemaId, Object... args)
            throws Exception {
        ByteBuffer buffer = createPacket(code, syncId, schemaId, args);

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
                            throw new TimeoutException(config.writeTimeoutMillis + "ms is exceeded while waiting for empty buffer you could configure write timeout it in TarantoolConfig");
                        }
                    } catch (InterruptedException e) {
                        throw new CommunicationException("Interrupted", e);
                    }
                }
                sharedBuffer.put(buffer);
                wait.incrementAndGet();
                bufferNotEmpty.signalAll();
                stats.buffered++;
            } finally {
                bufferLock.unlock();
            }
        } else {
            stats.sharedWriteLockTimeouts++;
            throw new TimeoutException(config.writeTimeoutMillis + "ms is exceeded while waiting for shared buffer lock you could configure write timeout in TarantoolConfig");
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
                    wait.incrementAndGet();
                } finally {
                    writeLock.unlock();
                }
                return true;
            } else {
                stats.directWriteLockTimeouts++;
                throw new TimeoutException(config.writeTimeoutMillis + "ms is exceeded while waiting for channel lock you could configure write timeout in TarantoolConfig");
            }
        }
        return false;
    }


    protected void readThread() {
        try {
            while (!Thread.currentThread().isInterrupted()) {
                try {
                    long code;
                    readPacket(is);
                    code = (Long) headers.get(Key.CODE.getId());
                    Long syncId = (Long) headers.get(Key.SYNC.getId());
                    FutureImpl<List<?>> future = futures.remove(syncId);
                    stats.received++;
                    wait.decrementAndGet();
                    complete(code, future);
                } catch (Exception e) {
                    die("Cant read answer", e);
                    return;
                }
            }
        } catch (Exception e) {
            die("Cant init thread", e);
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


    protected void fail(FutureImpl<List<?>> q, Exception e) {
        q.setError(e);
    }

    protected void complete(long code, FutureImpl<List<?>> q) {
        if (q != null) {
            if (code == 0) {
                q.setValue((List) body.get(Key.DATA.getId()));
            } else {
                Object error = body.get(Key.ERROR.getId());
                fail(q, serverError(code, error));
            }
        }
    }


    protected List syncGet(Future<List<?>> r) {
        try {
            return r.get();
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
        long code = 0;
        while (buffer.remaining() > 0 && (code = channel.write(buffer)) > -1) {
        }
        if (code < 0) {
            throw new SocketException("write failed code: " + code);
        }
    }


    @Override
    public void close() {
        if (connector != null) {
            connector.interrupt();
        }
        stopIO();
    }

    protected void stopIO() {
        if (reader != null) {
            reader.interrupt();
        }
        if (writer != null) {
            writer.interrupt();
        }
        if (is != null) {
            try {
                is.close();
            } catch (IOException ignored) {

            }
        }
        if (cis != null) {
            try {
                cis.close();
            } catch (IOException ignored) {

            }
        }
        closeChannel(channel);
        try {
            stopIO.await();
        } catch (InterruptedException ignored) {

        }
    }

    @Override
    public boolean isAlive() {
        return thumbstone == null;
    }

    @Override
    public void waitAlive() throws InterruptedException {
        while(!isAlive()) {
            alive.await();
        }
    }

    @Override
    public boolean waitAlive(long timeout, TimeUnit unit) throws InterruptedException {
        return alive.await(timeout, unit);
    }

    @Override
    public TarantoolClientOps<Integer, List<?>, Object, List<?>> syncOps() {
        return syncOps;
    }

    @Override
    public TarantoolClientOps<Integer, List<?>, Object, Future<List<?>>> asyncOps() {
        return this;
    }

    @Override
    public TarantoolClientOps<Integer, List<?>, Object, Long> fireAndForgetOps() {
        return fireAndForgetOps;
    }


    protected class SyncOps extends AbstractTarantoolOps<Integer, List<?>, Object, List<?>> {

        @Override
        public List exec(Code code, Object... args) {
            return syncGet(TarantoolClientImpl.this.exec(code, args));
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

    protected boolean isDead(FutureImpl<List<?>> q) {
        if (TarantoolClientImpl.this.thumbstone != null) {
            fail(q, new CommunicationException("Connection is dead", thumbstone));
            return true;
        }
        return false;
    }

    public Exception getThumbstone() {
        return thumbstone;
    }

    public TarantoolClientStats getStats() {
        return stats;
    }

}
