package org.tarantool;

import org.tarantool.server.*;

import java.io.*;
import java.net.*;
import java.nio.channels.*;

public class SimpleSocketChannelProvider implements SocketChannelProvider{

    private final TarantoolInstanceInfo tarantoolInstanceInfo;

    public SimpleSocketChannelProvider(InetSocketAddress socketAddress, String username, String password) {
        this.tarantoolInstanceInfo = TarantoolInstanceInfo.create(socketAddress, username, password);
    }

    public SimpleSocketChannelProvider(String address, String username, String password) {
        this.tarantoolInstanceInfo = TarantoolInstanceInfo.create(address, username, password);
    }

    @Override
    public SocketChannel get() {
        try {
            return SocketChannel.open(tarantoolInstanceInfo.getSocketAddress());
        } catch (IOException e) {
            String msg = "Exception occurred while connecting to instance " + tarantoolInstanceInfo;
            throw new CommunicationException(msg, e);
        }
    }
}
