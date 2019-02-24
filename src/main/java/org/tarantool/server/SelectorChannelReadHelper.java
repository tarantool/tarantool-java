package org.tarantool.server;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.nio.channels.spi.SelectorProvider;

class SelectorChannelReadHelper {
    private final SocketChannel channel;
    private final Selector selector;

    public SelectorChannelReadHelper(SocketChannel channel) throws IOException {
        if (channel.isBlocking()) {
            throw new IllegalArgumentException("Channel have to be blocking");
        }

        this.channel = channel;
        selector = SelectorProvider.provider().openSelector();
        channel.register(selector, SelectionKey.OP_READ);
    }

    public void readFully(ByteBuffer buffer) throws IOException {
        channel.read(buffer);

        while (buffer.remaining() > 0) {
            selector.select();//todo think about read timeout
            channel.read(buffer);
        }
    }

}
