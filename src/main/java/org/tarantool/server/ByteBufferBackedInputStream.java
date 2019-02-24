package org.tarantool.server;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;

class ByteBufferBackedInputStream extends InputStream {

    private final ByteBuffer buf;

    /**
     * //todo add a comment
     * @param buf a buffer have to be ready fo read (flipped)
     */
    public ByteBufferBackedInputStream(ByteBuffer buf) {
        this.buf = buf;
    }

    public int read() throws IOException {
        if (!buf.hasRemaining()) {
            return -1;
        }
        return buf.get() & 0xFF;
    }

    public int read(byte[] bytes, int off, int len)
            throws IOException {
        if (!buf.hasRemaining()) {
            return -1;
        }

        len = Math.min(len, buf.remaining());
        buf.get(bytes, off, len);
        return len;
    }

    @Override
    public int available() {
        return buf.remaining();
    }

    public boolean hasAvailable() {
        return available() > 0;
    }
}