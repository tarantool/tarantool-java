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

    public static String FAKE_WELCOME_STRING = "Tarantool 1.10.2 (Binary) 53b5547d-0560-4383-b303-4861572d4517\n" +
            "1bsTc5Ibljs94bsexVze+ZngV1vBJcstoYDxSTa9h8k=";
    private boolean isWellcomePerformed = false;

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
        synchronized (this) {
            if (isWellcomePerformed) {
                return dodgeRead(dst);
            } else {
                byte[] bytes = FAKE_WELCOME_STRING.getBytes();
                dst.put(bytes);
                isWellcomePerformed = true;
                return bytes.length;
            }
        }
    }

    private int dodgeRead(ByteBuffer dst) {
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
        synchronized (this) {
            if (isWellcomePerformed) {
                blockingQueue.add(src.array());
            }
            return src.array().length;
        }
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
