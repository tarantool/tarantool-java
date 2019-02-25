package org.tarantool.server;

import java.net.*;
import java.nio.channels.*;
import java.util.*;

/**
 * Holds info about a tarantool node.
 */
public class TarantoolNodeInfo {

    private final InetSocketAddress socketAddress;
    private final String username;
    private final String password;

    private TarantoolNodeInfo(InetSocketAddress socketAddress, String username, String password) {
        this.socketAddress = socketAddress;
        this.username = username;
        this.password = password;
    }

    /**
     * @return A socket address that the client can be connected to
     */
    public InetSocketAddress getSocketAddress() {
        return socketAddress;
    }

    public String getUsername() {
        return username;
    }

    public String getPassword() {
        return password;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TarantoolNodeInfo node = (TarantoolNodeInfo) o;
        return socketAddress.equals(node.socketAddress);
    }

    @Override
    public int hashCode() {
        return Objects.hash(socketAddress);
    }

    /**
     *
     * @param socketAddress Nonnull socket address
     * @return Instance of {@link TarantoolNodeInfo}
     */
    public static TarantoolNodeInfo create(InetSocketAddress socketAddress, String username, String password) {
        if (socketAddress == null) {
            throw new IllegalArgumentException("A socket address can not be null.");
        }

        return new TarantoolNodeInfo(socketAddress, username, password);
    }


    /**
     * @param address hostname address as String
     *
     * @throws IllegalArgumentException if the port parameter is outside the range
     *                                  of valid port values, or if the hostname parameter is <TT>null</TT>.
     * @throws SecurityException        if a security manager is present and
     *                                  permission to resolve the host name is
     *                                  denied.
     */
    public static TarantoolNodeInfo create(String address, String username, String password) {
        if (address == null) {
            throw new IllegalArgumentException("A hostname address can not be null.");
        }

        return new TarantoolNodeInfo(parseAddress(address), username, password);
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

        int port;
        try {
            port = (idx < 0) ? 3301 : Integer.parseInt(addr.substring(idx + 1));
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Exception while parsing port in address '" + addr + "'", e);
        }

        return new InetSocketAddress(host, port);
    }

    @Override
    public String toString() {
        return "TarantoolNodeInfo{" +
                "socketAddress=" + socketAddress +
                ", username='" + username + '\'' +
                ", password='" + password + '\'' +
                '}';
    }
}
