package org.tarantool;

import org.tarantool.server.*;

import java.io.*;
import java.net.*;
import java.nio.*;
import java.nio.channels.*;
import java.util.*;
import java.util.stream.*;

public class SingleNodeCommunicationProvider implements NodeCommunicationProvider {

    private final TarantoolInstanceInfo tarantoolInstanceInfo;

    private TarantoolInstanceConnection nodeConnection;

    public SingleNodeCommunicationProvider(InetSocketAddress socketAddress, String username, String password) {
        this.tarantoolInstanceInfo = TarantoolInstanceInfo.create(socketAddress, username, password);
    }

    public SingleNodeCommunicationProvider(String address, String username, String password) {
        this.tarantoolInstanceInfo = TarantoolInstanceInfo.create(address, username, password);
    }

    @Override
    public void connect() throws IOException {
        nodeConnection = TarantoolInstanceConnection.connect(tarantoolInstanceInfo);
    }

    public void writeBuffer(ByteBuffer byteBuffer) throws IOException {
        SocketChannel channel2Write = getChannel();
        BinaryProtoUtils.writeFully(channel2Write, byteBuffer);
    }

    public TarantoolBinaryPackage readPackage() throws IOException {
        SocketChannel channel2Read = getChannel();
        return BinaryProtoUtils.readPacket(channel2Read);
    }

    private SocketChannel getChannel() {
        if (nodeConnection == null) {
            throw new IllegalStateException("Not initialized");
        }
        return nodeConnection.getChannel();
    }

    @Override
    public String getDescription() {
        if (nodeConnection != null) {
            return nodeConnection.getNodeInfo().getSocketAddress().toString();
        } else {
            return "Unconnected. Node " + tarantoolInstanceInfo.getSocketAddress().toString();
        }
    }
}
