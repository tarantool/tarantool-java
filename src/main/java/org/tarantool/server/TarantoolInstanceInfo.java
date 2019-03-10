package org.tarantool.server;

import java.net.*;
import java.util.*;

/**
 * Holds info about a tarantool instance.
 */
public class TarantoolInstanceInfo {

    private final InetSocketAddress socketAddress;
    private final String username;
    private final String password;

    private TarantoolInstanceInfo(InetSocketAddress socketAddress, String username, String password) {
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
        TarantoolInstanceInfo node = (TarantoolInstanceInfo) o;
        return socketAddress.equals(node.socketAddress);
    }

    @Override
    public int hashCode() {
        return Objects.hash(socketAddress);
    }

    /**
     *
     * @param socketAddress Nonnull socket address
     * @return Instance of {@link TarantoolInstanceInfo}
     */
    public static TarantoolInstanceInfo create(InetSocketAddress socketAddress, String username, String password) {
        if (socketAddress == null) {
            throw new IllegalArgumentException("A socket address can not be null.");
        }

        return new TarantoolInstanceInfo(socketAddress, username, password);
    }


    /**
     * Creates an instance info object with no authentication data.
     *
     * @param address hostname address as String
     *
     * @throws IllegalArgumentException if the port parameter is outside the range
     *                                  of valid port values, or if the hostname parameter is <TT>null</TT>.
     * @throws SecurityException        if a security manager is present and
     *                                  permission to resolve the host name is
     *                                  denied.
     */
    public static TarantoolInstanceInfo create(String address) {
        return create(address, null, null);
    }



    /**
     * Creates an instance info object
     * @param address hostname address as String
     * @param username authentication username
     * @param password authentication password
     *
     * @throws IllegalArgumentException if the port parameter is outside the range
     *                                  of valid port values, or if the hostname parameter is <TT>null</TT>.
     * @throws SecurityException        if a security manager is present and
     *                                  permission to resolve the host name is
     *                                  denied.
     */
    public static TarantoolInstanceInfo create(String address, String username, String password) {
        if (address == null) {
            throw new IllegalArgumentException("A hostname address can not be null.");
        }

        return new TarantoolInstanceInfo(parseAddress(address), username, password);
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
        return "TarantoolInstanceInfo{" +
                "socketAddress=" + socketAddress +
                ", username='" + username + '\'' +
                ", password='" + password + '\'' +
                '}';
    }
}
