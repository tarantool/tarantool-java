package org.tarantool;

import java.net.InetSocketAddress;

public class ReconnectingSocketProviderImpl extends ReconnectingSocketProvider {

    private String host;
    private int port;

    /**
     * Returns the IP address or hostname of a Tarantool server
     * @return {@link java.lang.String}
     */
    public String getHost() {
        return host;
    }

    public ReconnectingSocketProviderImpl setHost(String host) {
        if (host == null || host.isEmpty()) {
            throw new IllegalArgumentException("Tarantool server host is empty");
        }
        this.host = host;
        return this;
    }

    /**
     * Returns the Tarantool server port
     * @return {@code int}
     */
    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        if (port <= 0) {
            throw new IllegalArgumentException("Tarantool server port is less or equal to 0");
        }
        this.port = port;
    }

    public ReconnectingSocketProviderImpl(String host, int port) {
        if (host == null || host.isEmpty()) {
            throw new IllegalArgumentException("Tarantool server host is empty");
        }
        if (port <= 0) {
            throw new IllegalArgumentException("Tarantool server port is less or equal to 0");
        }
        this.host = host;
        this.port = port;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public InetSocketAddress getSocketAddress() {
        return new InetSocketAddress(this.host, this.port);
    }
}
