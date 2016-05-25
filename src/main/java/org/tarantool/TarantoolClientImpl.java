package org.tarantool;

import org.tarantool.async.AsyncQuery;
import org.tarantool.schema.TarantoolConnectionSchemaAware;

import java.io.*;
import java.net.InetSocketAddress;
import java.net.SocketException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class TarantoolClientImpl extends AbstractTarantoolConnection16<Integer, Object, Object, Future<List>> implements TarantoolConnectionSchemaAware, TarantoolClient, TarantoolConnection16Ops<Integer, Object, Object, Future<List>> {
    protected SocketChannel channel;
    protected String salt;

    protected AtomicLong syncId = new AtomicLong();
    protected Map<Long, AsyncQuery<List>> futures = new ConcurrentHashMap<Long, AsyncQuery<List>>(1024 * 16);

    protected int msgPackOptions = MsgPackLite.OPTION_UNPACK_NUMBER_AS_LONG | MsgPackLite.OPTION_UNPACK_RAW_AS_STRING;

    /**
     * Read properties
     */
    protected DataInputStream is;
    protected long bytesRead;
    protected Map<Integer, Object> headers;
    protected Map<Integer, Object> body;

    /**
     * Write properties
     */
    final Lock writeLock = new ReentrantLock();
    final Condition flushDone = writeLock.newCondition();
    protected ByteBuffer outBuffer;
    protected volatile long lastFlush;
    private final long batchTimeout;

    /**
     * Interfaces
     */
    protected SyncOps syncOps = new SyncOps();
    protected FireAndForgetOps fireAndForgetOps = new FireAndForgetOps();


    protected Thread reader = new Thread(new Runnable() {
        @Override
        public void run() {
            readThread();
        }
    });

    protected Thread flusher = new Thread(new Runnable() {
        @Override
        public void run() {
            flushThread();
        }
    });


    private String host;
    private int port;
    private boolean isAutoReconnect = false;
    /**
     * reconnect vals
     **/
    protected long reconnectBackoffMs = 1000;
    private long lastConnectAttemptMs = 0;


    public TarantoolClientImpl(SocketChannel channel, long batchTimeout, int predictedFutures) {
        this.batchTimeout = batchTimeout;
        this.channel = channel;

        initiateConnect();

        reader.start();
        if (batchTimeout > 0) {
            flusher.start();
        }
    }

    public TarantoolClientImpl(String host, int port, long batchTimeout) throws IOException {
        this.host = host;
        this.port = port;
        this.isAutoReconnect = true;
        this.batchTimeout = batchTimeout;

        this.channel = SocketChannel.open(new InetSocketAddress(host, port));
        initiateConnect();

        reader.start();
        if (batchTimeout > 0) {
            flusher.start();
        }
    }

    protected void initiateConnect() {
        try {
            this.outBuffer = batchTimeout > 0 ? ByteBuffer.allocateDirect(channel.socket().getSendBufferSize()) : null;
            final InputStream inputStream = new BufferedInputStream(channel.socket().getInputStream(), channel.socket().getReceiveBufferSize());
            InputStream counter = new InputStream() {
                @Override
                public int read() throws IOException {
                    bytesRead++;
                    return inputStream.read();
                }

                @Override
                public int read(byte[] b, int off, int len) throws IOException {
                    int read = inputStream.read(b, off, len);
                    bytesRead += read;
                    return read;
                }
            };
            is = new DataInputStream(counter);
            byte[] bytes = new byte[64];
            is.readFully(bytes);
            String firstLine = new String(bytes);
            if (!firstLine.startsWith("Tarantool")) {
                channel.close();
                throw new CommunicationException("Welcome message should starts with tarantool but starts with '" + firstLine + "'");
            }
            is.readFully(bytes);
            this.salt = new String(bytes);
        } catch (IOException e) {
            throw new CommunicationException("Can't connect with tarantool", e);
        }
        try {
            channel.socket().setSoTimeout(0);
        } catch (SocketException e) {
            throw new CommunicationException("Couldn't disable read timeout", e);
        }
    }


    private boolean isConnected() {
        return channel.isConnected();
    }

    private synchronized boolean socketReconnect() {
        if (!isConnected() && isAutoReconnect) {
            while (!isConnected()) {
                try {
                    if (new java.util.Date().getTime() - lastConnectAttemptMs >= this.reconnectBackoffMs) {
                        System.out.println("Reconnect...");
                        this.lastConnectAttemptMs = new Date().getTime();
                        this.channel = SocketChannel.open(new InetSocketAddress(host, port));
                        initiateConnect();
                        System.out.println("Reconnect... ok");
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
        return isConnected();
    }

    @Override
    public AsyncQuery<List> exec(Code code, Object... args) {
        AsyncQuery<List> q = new AsyncQuery<List>(syncId.incrementAndGet(), code, args);
        futures.put(q.getId(), q);
        try {
            write(code, q.getId(), args);
        } catch (Exception e) {
            q.setError(e);
        }
        return q;
    }


    public void ping() {
        syncGet(exec(Code.PING));
    }


    public void auth(String username, final String password) {
        try {
            final MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
            List auth = new ArrayList(2);
            auth.add("chap-sha1");

            byte[] p = sha1.digest(password.getBytes());

            sha1.reset();
            byte[] p2 = sha1.digest(p);

            sha1.reset();
            sha1.update(Base64.decode(salt), 0, 20);
            sha1.update(p2);
            byte[] scramble = sha1.digest();
            for (int i = 0, e = 20; i < e; i++) {
                p[i] ^= scramble[i];
            }
            auth.add(p);
            syncGet(exec(Code.AUTH, Key.USER_NAME, username, Key.TUPLE, auth));

        } catch (NoSuchAlgorithmException e) {
            throw new CommunicationException("Can't use sha-1", e);
        }
    }


    @Override
    public Long getSchemaId() {
        return (Long) headers.get(Key.SCHEMA_ID);
    }

    private void write(Code code, Long syncId, Object... args) throws IOException {
        ByteArrayOutputStream bos = new ByteArrayOutputStream(128);
        DataOutputStream ds = new DataOutputStream(bos);
        Map<Key, Object> header = new EnumMap<Key, Object>(Key.class);
        Map<Key, Object> body = new EnumMap<Key, Object>(Key.class);
        header.put(Key.CODE, code);
        header.put(Key.SYNC, syncId);
        if (args != null) {
            for (int i = 0, e = args.length; i < e; i += 2) {
                Object value = args[i + 1];
                body.put((Key) args[i], value);
            }
        }
        MsgPackLite.pack(header, ds);
        MsgPackLite.pack(body, ds);
        ds.flush();
        ByteBuffer sizeBuffer = ByteBuffer.allocate(5);
        sizeBuffer.put((byte) 0xce);
        sizeBuffer.putInt(bos.size()).position(5);
        sizeBuffer.flip();
        writeLock.lock();
        try {
            send(sizeBuffer);
            send(bos.toByteBuffer());
        } finally {
            writeLock.unlock();
        }
    }

    private void send(ByteBuffer buffer) throws IOException {
        if (outBuffer != null) {
            if (buffer.remaining() > outBuffer.remaining()) {
                flush();
                flushDone.signal();
                writeFully(buffer);
                lastFlush = System.currentTimeMillis();
            } else {
                outBuffer.put(buffer);
            }
        } else {
            writeFully(buffer);
        }

    }

    private void flush() throws IOException {
        if (outBuffer.position() > 0) {
            outBuffer.flip();
            writeFully(outBuffer);
            outBuffer.clear();
        }
    }

    private void writeFully(ByteBuffer buffer) throws IOException {
        boolean isComplete = false;
        while (!isComplete) {
            try {
                int code = 0;
                while (buffer.remaining() > 0 && (code = channel.write(buffer)) > -1) {

                }
                if (code < 0) {
                    channel.close();
                    throw new CommunicationException("Can't write bytes");
                }
                isComplete = true;
            } catch (CommunicationException e) {
                socketReconnect();
            }
        }
    }

    protected void readThread() {
        while (!Thread.interrupted()) {
            try {
                long code;
                int size = ((Number) MsgPackLite.unpack(is, msgPackOptions)).intValue();
                long mark = bytesRead;
                is.mark(size);
                headers = (Map<Integer, Object>) MsgPackLite.unpack(is, msgPackOptions);
                if (bytesRead - mark < size) {
                    body = (Map<Integer, Object>) MsgPackLite.unpack(is, msgPackOptions);
                }
                is.skipBytes((int) (bytesRead - mark - size));
                code = (Long) headers.get(Key.CODE.getId());
                Long syncId = (Long) headers.get(Key.SYNC.getId());
                AsyncQuery<List> future = futures.remove(syncId);
                complete(code, future);
            } catch (CommunicationException e) {
                try {
                    channel.close();
                    socketReconnect();
                } catch (IOException e1) {
                    e1.printStackTrace();
                }
                socketReconnect();
            } catch (IOException e) {
                if (Thread.interrupted()) {
                    return;
                } else {
                    throw new CommunicationException("Cant read bytes", e);
                }
            }
        }
    }

    protected void flushThread() {
        while (!Thread.interrupted()) {
            try {
                writeLock.lock();
                try {
                    flushDone.await(batchTimeout, TimeUnit.MILLISECONDS);
                    if (System.currentTimeMillis() - lastFlush >= batchTimeout) {
                        try {
                            flush();
                            lastFlush = System.currentTimeMillis();
                        } catch (IOException e) {
                            throw new CommunicationException("Can't write bytes", e);
                        }
                    }
                } finally {
                    writeLock.unlock();
                }
            } catch (InterruptedException e) {
                return;
            }
        }
    }

    protected void complete(long code, AsyncQuery<List> q) {
        if (q != null) {
            if (code == 0) {
                q.setValue((List) body.get(Key.DATA.getId()));
            } else {
                Object error = body.get(Key.ERROR.getId());
                q.setError(new TarantoolException((int) code, error instanceof String ? (String) error : new String((byte[]) error)));
            }
        }
    }

    protected List syncGet(AsyncQuery<List> r) {
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

    public int getMsgPackOptions() {
        return msgPackOptions;
    }

    public void setMsgPackOptions(int msgPackOptions) {
        this.msgPackOptions = msgPackOptions;
    }

    @Override
    public void close() {
        reader.interrupt();
        flusher.interrupt();
        try {
            channel.close();
        } catch (IOException ignored) {

        }
    }

    @Override
    public TarantoolConnection16Ops<Integer, Object, Object, List> syncOps() {
        return syncOps;
    }

    @Override
    public TarantoolConnection16Ops<Integer, Object, Object, Future<List>> asyncOps() {
        return this;
    }

    @Override
    public TarantoolConnection16Ops<Integer, Object, Object, Void> fireAndForgetOps() {
        return fireAndForgetOps;
    }

    protected class SyncOps extends AbstractTarantoolConnection16<Integer, Object, Object, List> implements TarantoolConnection16Ops<Integer, Object, Object, List> {
        @Override
        public List exec(Code code, Object... args) {
            return syncGet(TarantoolClientImpl.this.exec(code, args));
        }

        @Override
        public void auth(String username, String password) {
            TarantoolClientImpl.this.auth(username, password);
        }

        @Override
        public void close() {
            throw new IllegalStateException("You should close TarantoolClient to make this");
        }
    }

    protected class FireAndForgetOps extends AbstractTarantoolConnection16<Integer, Object, Object, Void> implements TarantoolConnection16Ops<Integer, Object, Object, Void> {
        @Override
        public Void exec(Code code, Object... args) {
            try {
                write(code, syncId.incrementAndGet(), args);
            } catch (Exception e) {

            }
            return null;
        }

        @Override
        public void auth(String username, String password) {
            TarantoolClientImpl.this.auth(username, password);
        }

        @Override
        public void close() {
            throw new IllegalStateException("You should close TarantoolClient to make this");
        }
    }


    protected class ByteArrayOutputStream extends java.io.ByteArrayOutputStream {
        public ByteArrayOutputStream(int size) {
            super(size);
        }

        ByteBuffer toByteBuffer() {
            return ByteBuffer.wrap(buf, 0, count);
        }
    }

}
