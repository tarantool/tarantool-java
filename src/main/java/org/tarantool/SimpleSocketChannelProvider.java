package org.tarantool;

import org.tarantool.server.*;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;

public class SimpleSocketChannelProvider implements SocketChannelProvider {

    private final TarantoolInstanceInfo tarantoolInstanceInfo;

    private TarantoolInstanceConnection nodeConnection;

    public SimpleSocketChannelProvider(InetSocketAddress socketAddress, String username, String password) {
        this.tarantoolInstanceInfo = TarantoolInstanceInfo.create(socketAddress, username, password);
    }

    public SimpleSocketChannelProvider(String address, String username, String password) {
        this.tarantoolInstanceInfo = TarantoolInstanceInfo.create(address, username, password);
    }

    @Override
    public void connect() throws IOException {
        nodeConnection = TarantoolInstanceConnection.connect(tarantoolInstanceInfo);
    }

    @Override
    public SocketChannel getNext() {
        try {
            return SocketChannel.open(tarantoolInstanceInfo.getSocketAddress());
        } catch (IOException e) {
            throw new CommunicationException("Exception occurred while connecting to node " + tarantoolInstanceInfo, e);
        }
    }

    public void writeBuffer(ByteBuffer byteBuffer) throws IOException {
        SocketChannel channel2Write = getChannel();
        BinaryProtoUtils.writeFully(channel2Write, byteBuffer);
    }

    public TarantoolBinaryPackage readPackage() throws IOException {
        SocketChannel channel2Read = getChannel();
        return BinaryProtoUtils.readPacket(channel2Read);
    }

    @Override
    public SocketChannel getChannel() {
        if (nodeConnection == null) {
            throw new IllegalStateException("Not initialized");
        }
        return nodeConnection.getChannel();
    }
}
