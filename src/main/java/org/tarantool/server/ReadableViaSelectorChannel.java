package org.tarantool.server;

import org.tarantool.CommunicationException;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.nio.channels.spi.SelectorProvider;

class ReadableViaSelectorChannel implements ReadableByteChannel {
    private final SocketChannel channel;
    private final Selector selector;

    public ReadableViaSelectorChannel(SocketChannel channel) throws IOException {
        if (channel.isBlocking()) {
            throw new IllegalArgumentException("Channel have to be non-blocking");
        }

        this.channel = channel;
        selector = SelectorProvider.provider().openSelector();
        channel.register(selector, SelectionKey.OP_READ);
    }

    @Override
    public int read(ByteBuffer buffer) throws IOException {
        int count, n;
        count = n = channel.read(buffer);

        if (n < 0) {
            throw new CommunicationException("Channel read failed " + n);
        }

        while (buffer.remaining() > 0) {
            selector.select();//todo think about read timeout
            n = channel.read(buffer);
            if (n < 0) {
                throw new CommunicationException("Channel read failed: " + n);
            }
            count += n;
        }
        return count;
    }

    @Override
    public boolean isOpen() {
        return channel.isOpen();
    }

    @Override
    public void close() throws IOException {
        selector.close();
        channel.close();
    }
}
