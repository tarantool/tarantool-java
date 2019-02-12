package org.tarantool;

import java.io.*;
import java.net.*;
import java.nio.*;
import java.nio.channels.*;
import java.nio.channels.spi.*;
import java.util.*;
import java.util.concurrent.*;

public class DodgeSocketChannel extends SocketChannel {

    private ArrayBlockingQueue<byte[]> blockingQueue;

    public DodgeSocketChannel(Integer queueSize) {
        super(null);
        this.blockingQueue = new ArrayBlockingQueue<>(queueSize);
    }

    /**
     * Initializes a new instance of this class.
     *
     * @param provider The provider that created this channel
     */
    protected DodgeSocketChannel(SelectorProvider provider) {
        super(provider);
    }

    @Override
    public SocketChannel bind(SocketAddress local) throws IOException {
        throw new UnsupportedOperationException("This operation is not implemented");
    }

    @Override
    public <T> SocketChannel setOption(SocketOption<T> name, T value) throws IOException {
        throw new UnsupportedOperationException("This operation is not implemented");
    }

    @Override
    public <T> T getOption(SocketOption<T> name) throws IOException {
        throw new UnsupportedOperationException("This operation is not implemented");
    }

    @Override
    public Set<SocketOption<?>> supportedOptions() {
        throw new UnsupportedOperationException("This operation is not implemented");
    }

    @Override
    public SocketChannel shutdownInput() throws IOException {
        throw new UnsupportedOperationException("This operation is not implemented");
    }

    @Override
    public SocketChannel shutdownOutput() throws IOException {
        throw new UnsupportedOperationException("This operation is not implemented");
    }

    @Override
    public Socket socket() {
        throw new UnsupportedOperationException("This operation is not implemented");
    }

    @Override
    public boolean isConnected() {
        throw new UnsupportedOperationException("This operation is not implemented");
    }

    @Override
    public boolean isConnectionPending() {
        throw new UnsupportedOperationException("This operation is not implemented");
    }

    @Override
    public boolean connect(SocketAddress remote) throws IOException {
        throw new UnsupportedOperationException("This operation is not implemented");
    }

    @Override
    public boolean finishConnect() throws IOException {
        throw new UnsupportedOperationException("This operation is not implemented");
    }

    @Override
    public SocketAddress getRemoteAddress() throws IOException {
        throw new UnsupportedOperationException("This operation is not implemented");
    }

    @Override
    public int read(ByteBuffer dst) throws IOException {
        try {
            byte[] bytes = blockingQueue.take();
            dst.put(bytes);
            return bytes.length;
        } catch (InterruptedException e) {
            throw new IllegalStateException(e);
        }
    }

    @Override
    public long read(ByteBuffer[] dsts, int offset, int length) throws IOException {
        throw new UnsupportedOperationException("This operation is not implemented");
    }

    @Override
    public int write(ByteBuffer src) throws IOException {
        blockingQueue.add(src.array());
        return src.array().length;
//        throw new UnsupportedOperationException("This operation is not implemented");
    }

    @Override
    public long write(ByteBuffer[] srcs, int offset, int length) throws IOException {
        throw new UnsupportedOperationException("This operation is not implemented");
    }

    @Override
    public SocketAddress getLocalAddress() throws IOException {
        throw new UnsupportedOperationException("This operation is not implemented");
    }

    @Override
    protected void implCloseSelectableChannel() throws IOException {

    }

    @Override
    protected void implConfigureBlocking(boolean block) throws IOException {

    }
}
