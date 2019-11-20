package org.tarantool.util;

/**
 * Tarantool instance container.
 */
public class NodeSpec {
    private final String host;
    private final Integer port;

    public NodeSpec(String host, Integer port) {
        this.host = host;
        this.port = port;
    }

    public String getHost() {
        return host;
    }

    public int getPort() {
        return port;
    }

    @Override
    public String toString() {
        return host + (port != null ? ":" + port : "");
    }
}
