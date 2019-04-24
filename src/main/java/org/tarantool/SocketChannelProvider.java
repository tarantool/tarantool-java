package org.tarantool;

import java.net.SocketAddress;
import java.nio.channels.SocketChannel;

public interface SocketChannelProvider {

    /**
     * Provides socket channel to init restore connection.
     * You could change hosts between retries in this method.
     *
     * @param retryNumber number of current retry.
     * @param lastError   the last error occurs when reconnecting
     *
     * @return the result of {@link SocketChannel#open(SocketAddress)} call
     *
     * @throws SocketProviderTransientException if recoverable error occurred
     * @throws RuntimeException                 if any other reasons occurred
     */
    SocketChannel get(int retryNumber, Throwable lastError);

}
