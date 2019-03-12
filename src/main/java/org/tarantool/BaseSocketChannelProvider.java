package org.tarantool;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.SocketChannel;

public abstract class BaseSocketChannelProvider implements SocketChannelProvider {

    /**
     * Limit of retries.
     */
    private int retriesLimit = RETRY_NO_LIMIT;

    /**
     * Timeout to establish socket connection with an individual server.
     */
    private int timeout = NO_TIMEOUT;

    /**
     * Tries to establish a new connection to the Tarantool instances.
     *
     * @param retryNumber number of current retry. Reset after successful connect.
     * @param lastError   the last error occurs when reconnecting
     *
     * @return connected socket channel
     *
     * @throws CommunicationException if any I/O errors happen or there are
     *                                no addresses available
     */
    @Override
    public final SocketChannel get(int retryNumber, Throwable lastError) {
        if (areRetriesExhausted(retryNumber)) {
            throw new CommunicationException("Connection retries exceeded.", lastError);
        }

        long deadline = System.currentTimeMillis() + timeout;
        while (!Thread.currentThread().isInterrupted()) {
            try {
                InetSocketAddress address = getAddress(retryNumber, lastError);
                return openChannel(address);
            } catch (IOException e) {
                checkTimeout(deadline, e);
            }
        }
        throw new CommunicationException("Thread interrupted.", new InterruptedException());
    }

    private void checkTimeout(long deadline, Exception e) {
        long timeLeft = deadline - System.currentTimeMillis();
        if (timeLeft <= 0) {
            throw new CommunicationException("Connection time out.", e);
        }
        try {
            Thread.sleep(timeLeft / 10);
        } catch (InterruptedException ignored) {
            Thread.currentThread().interrupt();
        }
    }

    /**
     * Gets address to be used to establish a new connection
     * Address can be null.
     *
     * @param retryNumber reconnection attempt number
     * @param lastError   reconnection reason
     *
     * @return available address which is depended on implementation
     *
     * @throws IOException if any I/O errors occur
     */
    protected abstract InetSocketAddress getAddress(int retryNumber, Throwable lastError) throws IOException;

    /**
     * Sets maximum amount of reconnect attempts to be made before an exception is raised.
     * The retry count is maintained by a {@link #get(int, Throwable)} caller
     * when a socket level connection was established.
     * <p>
     * Negative value means unlimited attempts.
     *
     * @param retriesLimit Limit of retries to use.
     */
    public void setRetriesLimit(int retriesLimit) {
        this.retriesLimit = retriesLimit;
    }

    /**
     * Gets limit of attempts to establish connection.
     *
     * @return Maximum reconnect attempts to make before raising exception.
     */
    public int getRetriesLimit() {
        return retriesLimit;
    }

    /**
     * Parse a string address in the form of host[:port]
     * and builds a socket address.
     *
     * @param address Server address.
     *
     * @return Socket address.
     */
    protected InetSocketAddress parseAddress(String address) {
        int separatorPosition = address.indexOf(':');
        String host = (separatorPosition < 0) ? address : address.substring(0, separatorPosition);
        int port = (separatorPosition < 0) ? 3301 : Integer.parseInt(address.substring(separatorPosition + 1));
        return new InetSocketAddress(host, port);
    }

    protected SocketChannel openChannel(InetSocketAddress socketAddress) throws IOException {
        SocketChannel channel = null;
        try {
            channel = SocketChannel.open();
            channel.socket().connect(socketAddress, timeout);
            return channel;
        } catch (IOException e) {
            if (channel != null) {
                try {
                    channel.close();
                } catch (IOException ignored) {
                    // No-op.
                }
            }
            throw e;
        }
    }

    /**
     * Sets maximum amount of time to wait for a socket connection establishment
     * with an individual server.
     * <p>
     * Zero means infinite timeout.
     *
     * @param timeout timeout value, ms.
     *
     * @throws IllegalArgumentException if timeout is negative.
     */
    public void setTimeout(int timeout) {
        if (timeout < 0) {
            throw new IllegalArgumentException("timeout is negative.");
        }
        this.timeout = timeout;
    }

    /**
     * Gest maximum amount of time to wait for a socket
     * connection establishment with an individual server.
     *
     * @return timeout
     */
    public int getTimeout() {
        return timeout;
    }

    /**
     * Provides a decision on whether retries limit is hit.
     *
     * @param retries Current count of retries.
     *
     * @return {@code true} if retries are exhausted.
     */
    private boolean areRetriesExhausted(int retries) {
        int limit = getRetriesLimit();
        if (limit < 0) {
            return false;
        }
        return retries >= limit;
    }
}
