package org.tarantool.server;

import org.tarantool.CommunicationException;

import java.io.Closeable;
import java.io.IOException;
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

    private TarantoolInstanceConnection(TarantoolInstanceInfo nodeInfo,
                                        TarantoolInstanceConnectionMeta meta,
                                        SocketChannel channel) {
        this.nodeInfo = nodeInfo;
        this.meta = meta;
        this.channel = channel;
    }

    /**
     *  Attempts to connect to a tarantool instance and create correspond TarantoolInstanceConnection
     *
     * @param tarantoolInstanceInfo
     * @throws CommunicationException
     */
    public static TarantoolInstanceConnection connect(TarantoolInstanceInfo tarantoolInstanceInfo) throws IOException {
        SocketChannel channel;
        try {
            channel = SocketChannel.open(tarantoolInstanceInfo.getSocketAddress());
            String username = tarantoolInstanceInfo.getUsername();
            String password = tarantoolInstanceInfo.getPassword();

            TarantoolInstanceConnectionMeta meta = BinaryProtoUtils.connect(channel, username, password);

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

    private void closeConnection() {
        if (channel != null) {
            try {
                channel.close();
            } catch (IOException ignored) {

            }
        }
    }

    @Override
    public void close() throws IOException {
        closeConnection();
    }
}
