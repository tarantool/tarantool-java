package org.tarantool;

import org.tarantool.server.TarantoolNode;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.SocketChannel;

public class SimpleSocketChannelProvider implements SocketChannelProvider{

    private final TarantoolNode tarantoolNode;

    public SimpleSocketChannelProvider(InetSocketAddress socketAddress) {
        this.tarantoolNode = TarantoolNode.create(socketAddress);
    }

    public SimpleSocketChannelProvider(String address) {
        this.tarantoolNode = TarantoolNode.create(address);
    }

    @Override
    public SocketChannel get(int retryNumber, Throwable lastError) {
        try {
            return SocketChannel.open(tarantoolNode.getSocketAddress());
        } catch (IOException e) {
            throw new CommunicationException("Exception occurred while connecting to node " + tarantoolNode, e);
        }
    }
}
