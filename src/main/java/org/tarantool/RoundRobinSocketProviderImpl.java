package org.tarantool;

import org.tarantool.server.*;

import java.io.*;
import java.net.*;
import java.nio.channels.*;

/**
 * Basic reconnection strategy that changes addresses in a round-robin fashion.
 * To be used with {@link TarantoolClientImpl}.
 */
public class RoundRobinSocketProviderImpl implements SocketChannelProvider {

    /** Timeout to establish socket connection with an individual server. */
    private int timeout; // 0 is infinite.

    /** Limit of retries. */
    private int retriesLimit = -1; // No-limit.

    private final TarantoolNode[] nodes;

    /** Current position within {@link #nodes} array. */
    private int pos;

    /**
     * Constructs an instance.
     *
     * @param slaveHosts Array of addresses in a form of [host]:[port].
     */
    public RoundRobinSocketProviderImpl(String[] slaveHosts) {
        if (slaveHosts == null || slaveHosts.length < 1) {
            throw new IllegalArgumentException("slave hosts is null ot empty");
        }

        nodes = new TarantoolNode[slaveHosts.length];
        for (int i = 0; i < slaveHosts.length; i++) {
            String slaveHostAddress = slaveHosts[i];
            nodes[i] = TarantoolNode.create(slaveHostAddress);
        }
    }

    /**
     * @return Non-empty list of round-robined nodes
     */
    public TarantoolNode[] getNodes() {
        return nodes;
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
    public RoundRobinSocketProviderImpl setTimeout(int timeout) {
        if (timeout < 0)
            throw new IllegalArgumentException("timeout is negative.");

        this.timeout = timeout;

        return this;
    }

    /**
     * @return Maximum amount of time to wait for a socket connection establishment
     *         with an individual server.
     */
    public int getTimeout() {
        return timeout;
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
    public RoundRobinSocketProviderImpl setRetriesLimit(int retriesLimit) {
        this.retriesLimit = retriesLimit;

        return this;
    }

    /**
     * @return Maximum reconnect attempts to make before raising exception.
     */
    public int getRetriesLimit() {
        return retriesLimit;
    }

    /** {@inheritDoc} */
    @Override
    public SocketChannel get(int retryNumber, Throwable lastError) {
        if (areRetriesExhausted(retryNumber)) {
            throw new CommunicationException("Connection retries exceeded.", lastError);
        }
        int attempts = getAddressCount();
        long deadline = System.currentTimeMillis() + timeout * attempts;
        while (!Thread.currentThread().isInterrupted()) {
            SocketChannel channel = null;
            try {
                InetSocketAddress addr = getNextSocketAddress();

                channel = SocketChannel.open();
                channel.socket().connect(addr, timeout);
                return channel;
            } catch (IOException e) {
                if (channel != null) {
                    try {
                        channel.close();
                    } catch (IOException ignored) {
                        // No-op.
                    }
                }
                long now = System.currentTimeMillis();
                if (deadline <= now) {
                    throw new CommunicationException("Connection time out.", e);
                }
                if (--attempts == 0) {
                    // Tried all addresses without any lack, but still have time.
                    attempts = getAddressCount();
                    try {
                        Thread.sleep((deadline - now) / attempts);
                    } catch (InterruptedException ignored) {
                        Thread.currentThread().interrupt();
                    }
                }
            }
        }
        throw new CommunicationException("Thread interrupted.", new InterruptedException());
    }

    /**
     * @return Number of configured addresses.
     */
    protected int getAddressCount() {
        return nodes.length;
    }

    /**
     * @return Socket address to use for the next reconnection attempt.
     */
    protected InetSocketAddress getNextSocketAddress() {
        InetSocketAddress res = nodes[pos].getSocketAddress();
        pos = (pos + 1) % nodes.length;
        return res;
    }

    /**
     * Parse a string address in the form of [host]:[port]
     * and builds a socket address.
     *
     * @param addr Server address.
     * @return Socket address.
     */
    protected InetSocketAddress parseAddress(String addr) {
        int idx = addr.indexOf(':');
        String host = (idx < 0) ? addr : addr.substring(0, idx);
        int port = (idx < 0) ? 3301 : Integer.parseInt(addr.substring(idx + 1));
        return new InetSocketAddress(host, port);
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
}
