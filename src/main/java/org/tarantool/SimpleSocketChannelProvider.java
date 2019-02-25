package org.tarantool;

import org.tarantool.server.*;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.SocketChannel;

public class SimpleSocketChannelProvider implements SocketChannelProvider{

    private final TarantoolNodeInfo tarantoolNodeInfo;

    private TarantoolNodeConnection nodeConnection;

    public SimpleSocketChannelProvider(InetSocketAddress socketAddress, String username, String password) {
        this.tarantoolNodeInfo = TarantoolNodeInfo.create(socketAddress, username, password);
    }

    public SimpleSocketChannelProvider(String address, String username, String password) {
        this.tarantoolNodeInfo = TarantoolNodeInfo.create(address, username, password);
    }

    @Override
    public void connect() {
        nodeConnection = TarantoolNodeConnection.connect(tarantoolNodeInfo);
    }

    @Override
    public SocketChannel getNext() {
        try {
            return SocketChannel.open(tarantoolNodeInfo.getSocketAddress());
        } catch (IOException e) {
            throw new CommunicationException("Exception occurred while connecting to node " + tarantoolNodeInfo, e);
        }
    }

    @Override
    public SocketChannel getChannel() {
        if (nodeConnection == null) {
            throw new IllegalStateException("Not initialized");
        }
        return null;
    }
}
