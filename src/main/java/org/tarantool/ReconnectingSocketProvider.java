package org.tarantool;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketTimeoutException;
import java.nio.channels.SocketChannel;

/**
 * Basic SocketChannelProvider implementation with the ability of reconnecting after failure.
 * To be used with {@link TarantoolClientImpl}.
 */
public abstract class ReconnectingSocketProvider implements SocketChannelProvider {
    /** Timeout to establish socket connection with an individual server. */
    private int timeout; // 0 is infinite.
    /** Limit of retries. */
    private int retriesLimit = -1; // No-limit.

    /**
     * @return Maximum amount of time to wait for a socket connection establishment
     *         with an individual server.
     */
    public int getTimeout() {
        return timeout;
    }

    /**
     * Sets maximum amount of time to wait for a socket connection establishment
     * with an individual server.
     *
     * Zero means infinite timeout.
     *
     * @param timeout Timeout value, ms.
     * @return {@code this}.
     * @throws IllegalArgumentException If timeout is negative.
     */
    public ReconnectingSocketProvider setTimeout(int timeout) {
        if (timeout < 0) {
            throw new IllegalArgumentException("timeout is negative");
        }
        this.timeout = timeout;
        return this;
    }

    /**
     * @return Maximum reconnect attempts to make before raising exception.
     */
    public int getRetriesLimit() {
        return retriesLimit;
    }

    /**
     * Sets maximum amount of reconnect attempts to be made before an exception is raised.
     * The retry count is maintained by a {@link #get(int, Throwable)} caller
     * when a socket level connection was established.
     *
     * Negative value means unlimited.
     *
     * @param retriesLimit Limit of retries to use.
     * @return {@code this}.
     */
    public ReconnectingSocketProvider setRetriesLimit(int retriesLimit) {
        this.retriesLimit = retriesLimit;
        return this;
    }

    /**
     * Provides a decision on whether retries limit is hit.
     *
     * @param retries Current count of retries.
     * @return {@code true} if retries are exhausted.
     */
    private boolean areRetriesExhausted(int retries) {
        int limit = getRetriesLimit();
        if (limit < 0)
            return false;
        return retries >= limit;
    }

    /**
     * Return a configured socket address where a Tarantool instance is listening to
     * @return {@link java.net.InetSocketAddress}
     */
    abstract InetSocketAddress getSocketAddress();

    /** {@inheritDoc} */
    @Override
    public SocketChannel get(int retryNumber, Throwable lastError) {
        if (areRetriesExhausted(retryNumber)) {
            throw new CommunicationException("Connection retries exceeded.", lastError);
        }
        try (SocketChannel channel = SocketChannel.open()) {
            InetSocketAddress addr = getSocketAddress();
            channel.socket().connect(addr, timeout);
            return channel;
        } catch (SocketTimeoutException e) {
            throw new CommunicationException("Connection timed out", e);
        } catch (IOException e) {
            throw new CommunicationException("Failed to establish a connection", e);
        }
    }
}
