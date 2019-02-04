package org.tarantool.server;

import org.tarantool.ByteBufferInputStream;
import org.tarantool.CommunicationException;
import org.tarantool.CountInputStream;
import org.tarantool.Key;
import org.tarantool.MsgPackLite;
import org.tarantool.SocketChannelProvider;
import org.tarantool.TarantoolClientConfig;
import org.tarantool.TarantoolException;

import java.io.DataInputStream;
import java.io.IOException;
import java.net.SocketException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.Map;

public class TarantoolServer {

    /**
     * External
     */
    private SocketChannelProvider socketProvider;
    private TarantoolClientConfig config;

    private CountingInputStream dis;
    protected SocketChannel channel;

    private MsgPackLite msgPackLite = MsgPackLite.INSTANCE;

    /**
     * Connection state
     */
    protected String salt;

    public TarantoolServer(SocketChannelProvider socketProvider, TarantoolClientConfig config) {
        this.socketProvider = socketProvider;

        this.config = config;
    }

    public void init() throws Exception {
        connect();
    }

    protected void connect() throws Exception {
        connect(socketProvider.get(0));
    }

    public static final class CountingInputStream extends DataInputStream {

        private final ByteBufferInputStream in;

        public CountingInputStream(ByteBufferInputStream byteBufferInputStream) {
            super(byteBufferInputStream);
            this.in = byteBufferInputStream;
        }

        private long getBytesRead() {
            return in.getBytesRead();
        }
    }

    protected void connect(final SocketChannel channel) throws Exception {
        ByteBufferInputStream bufferInputStream = null;
        try {
            ;
            CountingInputStream dis = new CountingInputStream((bufferInputStream = new ByteBufferInputStream(channel)));
            byte[] bytes = new byte[64];
            dis.readFully(bytes);
            String firstLine = new String(bytes);
            if (!firstLine.startsWith("Tarantool")) {
                CommunicationException e = new CommunicationException("Welcome message should starts with tarantool " +
                        "but starts with '" + firstLine + "'", new IllegalStateException("Invalid welcome packet"));

                throw e;
            }
            dis.readFully(bytes);
            String salt = new String(bytes);
            if (config.username != null && config.password != null) {
                writeFully(channel, BinaryProtoUtils.createAuthPacket(config.username, config.password, salt));
                TarantoolBinaryPackage biPack = readPacket();
                Long code = (Long) biPack.getHeaders().get(Key.CODE.getId());
                if (code != 0) {
                    throw serverError(code, biPack.getBody().get(Key.ERROR.getId()));
                }
            }

            this.dis = dis;
        } catch (IOException e) {
            try {
                if (bufferInputStream != null) {
                    bufferInputStream.close();
                }
            } catch (IOException ignored) {

            }
            throw new CommunicationException("Couldn't connect to tarantool", e);
        }
        channel.configureBlocking(false);
    }

    protected TarantoolException serverError(long code, Object error) {
        return new TarantoolException(code, error instanceof String ? (String) error : new String((byte[]) error));
    }

    public TarantoolBinaryPackage readPacket() throws IOException {
        return readPacket(dis);
    }


    public void writeFully(ByteBuffer buffer) throws IOException {
        writeFully(channel, buffer);
    }

    private void writeFully(SocketChannel channel, ByteBuffer buffer) throws IOException {
        long code = 0;
        while (buffer.remaining() > 0 && (code = channel.write(buffer)) > -1) {
        }
        if (code < 0) {
            throw new SocketException("write failed code: " + code);
        }
    }

    public void closeConnection() {
        if (channel != null) {
            try {
                channel.close();
            } catch (IOException ignored) {

            }
        }
    }
}
