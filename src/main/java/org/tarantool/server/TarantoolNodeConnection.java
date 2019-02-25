package org.tarantool.server;

import org.tarantool.ByteBufferInputStream;
import org.tarantool.CommunicationException;

import java.io.DataInputStream;
import java.io.IOException;
import java.net.*;
import java.nio.channels.SocketChannel;

@Deprecated
public class TarantoolNodeConnection {

    /**
     * External
     */
    private final TarantoolNodeInfo nodeInfo;
    private final TarantoolNodeConnectionMeta meta;

    protected final SocketChannel channel;

    private TarantoolNodeConnection(TarantoolNodeInfo nodeInfo, TarantoolNodeConnectionMeta meta, SocketChannel channel) {
        this.nodeInfo = nodeInfo;
        this.meta = meta;
        this.channel = channel;
    }

    /**
     *
     *
     * @param tarantoolNodeInfo
     * @throws CommunicationException
     */
    public static TarantoolNodeConnection connect(TarantoolNodeInfo tarantoolNodeInfo) {
        SocketChannel channel;
        try {
            channel = SocketChannel.open(tarantoolNodeInfo.getSocketAddress());
            String username = tarantoolNodeInfo.getUsername();
            String password = tarantoolNodeInfo.getPassword();

            TarantoolNodeConnectionMeta meta = BinaryProtoUtils.connect(channel, username, password);

            return new TarantoolNodeConnection(tarantoolNodeInfo, meta, channel);
        } catch (IOException e) {
            throw new CommunicationException("Exception occurred while connecting to node " + tarantoolNodeInfo, e);
        }
    }

    @Deprecated
    public void closeConnection() {
        if (channel != null) {
            try {
                channel.close();
            } catch (IOException ignored) {

            }
        }
    }
}
