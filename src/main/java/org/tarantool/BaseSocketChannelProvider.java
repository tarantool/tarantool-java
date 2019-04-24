package org.tarantool;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.SocketChannel;

public abstract class BaseSocketChannelProvider implements ConfigurableSocketChannelProvider {

    /**
     * Limit of retries.
     */
    private int retriesLimit = RETRY_NO_LIMIT;

    /**
     * Timeout to establish socket connection with an individual server.
     */
    private int connectionTimeout = NO_TIMEOUT;

    /**
     * Tries to establish a new connection to the Tarantool instances.
     *
     * @param retryNumber number of current retry
     * @param lastError   the last error occurs when reconnecting
     *
     * @return connected socket channel
     *
     * @throws CommunicationException           if number of retries or socket timeout are exceeded
     * @throws SocketProviderTransientException if any I/O errors happen
     */
    @Override
    public final SocketChannel get(int retryNumber, Throwable lastError) {
        try {
            return makeAttempt(retryNumber, lastError);
        } catch (IOException e) {
            throw new SocketProviderTransientException("Couldn't connect to the server", e);
        }
    }

    /**
     * Obtains a connected socket channel.
     *
     * @param retryNumber reconnection attempt number
     * @param lastError   reconnection reason
     *
     * @return opened socket channel
     *
     * @throws IOException if any I/O errors occur
     */
    protected abstract SocketChannel makeAttempt(int retryNumber, Throwable lastError) throws IOException;

    /**
     * Sets maximum amount of reconnect attempts to be made before an exception is raised.
     * The retry count is maintained by a {@link #get(int, Throwable)} caller
     * when a socket level connection was established.
     * <p>
     * Negative value means unlimited attempts.
     *
     * @param retriesLimit Limit of retries to use.
     */
    @Override
    public void setRetriesLimit(int retriesLimit) {
        if (retriesLimit < 0) {
            throw new IllegalArgumentException("Retries count cannot be negative.");
        }
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
            channel.socket().connect(socketAddress, connectionTimeout);
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
     * Gets maximum amount of time to wait for a socket
     * connection establishment with an individual server.
     *
     * @return timeout
     */
    public int getConnectionTimeout() {
        return connectionTimeout;
    }

    /**
     * Sets maximum amount of time to wait for a socket connection establishment
     * with an individual server.
     * <p>
     * Zero means infinite connectionTimeout.
     *
     * @param connectionTimeout connectionTimeout value, ms.
     *
     * @throws IllegalArgumentException if connectionTimeout is negative.
     */
    @Override
    public void setConnectionTimeout(int connectionTimeout) {
        if (connectionTimeout < 0) {
            throw new IllegalArgumentException("Connection timeout cannot be negative.");
        }
        this.connectionTimeout = connectionTimeout;
    }

}
