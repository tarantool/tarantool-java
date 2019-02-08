package org.tarantool.server;

import java.net.*;

/**
 * Holds info about a tarantool node.
 */
public class TarantoolNode {

    private final InetSocketAddress socketAddress;

    private TarantoolNode(InetSocketAddress socketAddress) {
        this.socketAddress = socketAddress;
    }

    /**
     * @return A socket address that the client can be connected to
     */
    public InetSocketAddress getSocketAddress() {
        return socketAddress;
    }

    /**
     *
     * @param socketAddress Nonnull socket address
     * @return Instance of {@link TarantoolNode}
     */
    public static TarantoolNode create(InetSocketAddress socketAddress) {
        if (socketAddress == null) {
            throw new IllegalArgumentException("A socket address can not be null.");
        }

        return new TarantoolNode(socketAddress);
    }


    /**
     * @param address hostname addess as String
     *
     * @throws IllegalArgumentException if the port parameter is outside the range
     *                                  of valid port values, or if the hostname parameter is <TT>null</TT>.
     * @throws SecurityException        if a security manager is present and
     *                                  permission to resolve the host name is
     *                                  denied.
     */
    public static TarantoolNode create(String address) {
        if (address == null) {
            throw new IllegalArgumentException("A hostname address can not be null.");
        }

        return new TarantoolNode(parseAddress(address));
    }

    /**
     * Parse a string address in the form of [host]:[port]
     * and builds a socket address.
     *
     * @param addr Server address as string.
     * @throws IllegalArgumentException if the port parameter is outside the range
     *                                  of valid port values, or if the hostname parameter is <TT>null</TT>.
     * @throws SecurityException        if a security manager is present and
     *                                  permission to resolve the host name is
     *                                  denied.
     */
    private static InetSocketAddress parseAddress(String addr) {
        int idx = addr.indexOf(':');
        String host = (idx < 0) ? addr : addr.substring(0, idx);
        int port = (idx < 0) ? 3301 : Integer.parseInt(addr.substring(idx + 1));
        return new InetSocketAddress(host, port);
    }
}
