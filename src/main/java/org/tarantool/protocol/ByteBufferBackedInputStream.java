package org.tarantool.protocol;

import java.io.InputStream;
import java.nio.ByteBuffer;

/**
 * Input stream based on ByteBuffer.
 */
class ByteBufferBackedInputStream extends InputStream {

    private final ByteBuffer buf;

    /**
     * Constructs a new wrapper-stream for {@link ByteBuffer}.
     *
     * @param buf a buffer that have to be ready for read (flipped)
     */
    public ByteBufferBackedInputStream(ByteBuffer buf) {
        this.buf = buf;
    }

    public int read() {
        if (!buf.hasRemaining()) {
            return -1;
        }
        return buf.get() & 0xFF;
    }

    public int read(byte[] bytes, int off, int len) {
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
