package org.tarantool.server;

import org.tarantool.CommunicationException;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;

public class TarantoolInstanceConnection implements Closeable {

    /**
     * Information about connection
     */
    private final TarantoolInstanceInfo nodeInfo;

    /**
     * Connection metadata
     */
    private final TarantoolInstanceConnectionMeta meta;

    /**
     * Nonnull connection to a tarantool instance
     */
    protected final SocketChannel channel;

    /**
     * Nonnull connection to a tarantool instance
     */
    protected final ReadableViaSelectorChannel readChannel;

    private TarantoolInstanceConnection(TarantoolInstanceInfo nodeInfo,
                                        TarantoolInstanceConnectionMeta meta,
                                        SocketChannel channel) throws IOException {
        this.nodeInfo = nodeInfo;
        this.meta = meta;
        this.channel = channel;
        this.readChannel = new ReadableViaSelectorChannel(channel);
    }

    /**
     *  Attempts to connect to a tarantool instance and create correspond TarantoolInstanceConnection
     *
     * @param tarantoolInstanceInfo information about an instance to connect
     * @throws CommunicationException if an error occurred during connection related exchanges
     * @throws IOException in case of any IO fail
     */
    public static TarantoolInstanceConnection connect(TarantoolInstanceInfo tarantoolInstanceInfo) throws IOException {
        SocketChannel channel;
        try {
            channel = SocketChannel.open(tarantoolInstanceInfo.getSocketAddress());

            String username = tarantoolInstanceInfo.getUsername();
            String password = tarantoolInstanceInfo.getPassword();
            TarantoolInstanceConnectionMeta meta = BinaryProtoUtils.connect(channel, username, password);

            channel.configureBlocking(false);

            return new TarantoolInstanceConnection(tarantoolInstanceInfo, meta, channel);
        } catch (IOException e) {
            throw new IOException("IOException occurred while connecting to node " + tarantoolInstanceInfo, e);
        }
    }

    public TarantoolInstanceInfo getNodeInfo() {
        return nodeInfo;
    }

    public TarantoolInstanceConnectionMeta getMeta() {
        return meta;
    }

    public SocketChannel getChannel() {
        return channel;
    }

    public void writeBuffer(ByteBuffer writerBuffer) throws IOException {
        BinaryProtoUtils.writeFully(getChannel(), writerBuffer);
    }

    public TarantoolBinaryPacket readPacket() throws IOException {
        return BinaryProtoUtils.readPacket(readChannel);
    }

    private void closeConnection() {
        try {
            readChannel.close();//also closes this.channel
        } catch (IOException ignored) {

        }
    }

    @Override
    public void close() throws IOException {
        closeConnection();
    }
}
