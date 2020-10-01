package org.tarantool.conversion;

import java.lang.ref.SoftReference;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.util.Objects;

/**
 * String decoders factory.
 *
 * <p>
 * The decoders are heavyweight objects to be created each time they are
 * requested. On the other hand, the decoders are not thread-safe and cannot
 * be reused at the same time.
 * This cache uses {@link java.lang.ThreadLocal} to keep a local copies of
 * the decoder per each tread.
 */
class StringDecoder {

    private static final ThreadLocal<SoftReference<CharsetDecoder>> decoderLocal = new ThreadLocal<>();

    /**
     * Gets a decoder of the specified charset.
     *
     * @param charset target charset
     *
     * @return decoder
     */
    public static CharsetDecoder getDecoder(Charset charset) {
        Objects.requireNonNull(charset);
        CharsetDecoder decoder = unwrap(decoderLocal);
        if (decoder == null) {
            decoder = charset.newDecoder();
            wrap(decoderLocal, decoder);
            return decoder;
        }
        if (!decoder.charset().equals(charset)) {
            decoder = charset.newDecoder();
            wrap(decoderLocal, decoder);
        }
        return decoder;
    }

    private static <T> void wrap(ThreadLocal<SoftReference<T>> local, T object) {
        local.set(new SoftReference<>(object));
    }

    private static <T> T unwrap(ThreadLocal<SoftReference<T>> local) {
        SoftReference<T> softReference = local.get();
        if (softReference == null) {
            return null;
        }
        return softReference.get();
    }

}
