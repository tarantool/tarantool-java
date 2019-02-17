package org.tarantool;

import org.tarantool.server.BinaryProtoUtils;
import org.tarantool.server.TarantoolNodeInfo;

import java.io.IOException;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

public abstract class TarantoolBase<Result> extends AbstractTarantoolOps<Integer, List<?>, Object, Result> {

    protected String serverVersion;
    /**
     * Connection state
     */
    protected String salt;
    protected MsgPackLite msgPackLite = MsgPackLite.INSTANCE;
    protected AtomicLong syncId = new AtomicLong();
    protected int initialRequestSize = 4096;

    public TarantoolBase() {
    }

    public TarantoolBase(String username, String password, Socket socket) {
        super();
        try {
            TarantoolNodeInfo info = BinaryProtoUtils.connect(socket, username, password);
            this.serverVersion = info.getServerVersion();
            this.salt = info.getSalt();
        } catch (CommunicationException e) {
            close();
            throw e;
        } catch (IOException e) {
            throw new CommunicationException("Couldn't connect to tarantool", e);
        }
    }

    protected static class SQLMetaData {
        protected String name;

        public SQLMetaData(String name) {
            this.name = name;
        }

        public String getName() {
            return name;
        }

        @Override
        public String toString() {
            return "SQLMetaData{" +
                    "name='" + name + '\'' +
                    '}';
        }
    }

    protected TarantoolException serverError(long code, Object error) {
        return new TarantoolException(code, error instanceof String ? (String) error : new String((byte[]) error));
    }

    protected class ByteArrayOutputStream extends java.io.ByteArrayOutputStream {
        public ByteArrayOutputStream(int size) {
            super(size);
        }

        ByteBuffer toByteBuffer() {
            return ByteBuffer.wrap(buf, 0, count);
        }
    }

    protected void closeChannel(SocketChannel channel) {
        if (channel != null) {
            try {
                channel.close();
            } catch (IOException ignored) {

            }
        }
    }

    protected void validateArgs(Object[] args) {
        if (args != null) {
            for (int i = 0; i < args.length; i += 2) {
                if (args[i + 1] == null) {
                    throw new NullPointerException(((Key) args[i]).name() + " should not be null");
                }
            }
        }
    }

    public void setInitialRequestSize(int initialRequestSize) {
        this.initialRequestSize = initialRequestSize;
    }

    public String getServerVersion() {
        return serverVersion;
    }
}
