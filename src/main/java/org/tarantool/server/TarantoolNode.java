package org.tarantool.server;

import org.tarantool.CommunicationException;

import java.io.IOException;
import java.net.*;
import java.nio.channels.SocketChannel;
import java.util.Objects;

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

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TarantoolNode node = (TarantoolNode) o;
        return socketAddress.equals(node.socketAddress);
    }

    @Override
    public int hashCode() {
        return Objects.hash(socketAddress);
    }

    @Deprecated
    private SocketChannel socketChannel;
    /**
     * Opens a socket connection channel to the tarantool node or returns an opened one
     * @return
     */
    @Deprecated //it's bad to open socket at level of this class
    public SocketChannel getSocketChannel(Integer timeout) {
        if (socketChannel == null) {
            socketChannel = openSocketChannel(timeout);
        }

        return socketChannel;
    }

    @Deprecated //it's bad to open socket at level of this class
    private SocketChannel openSocketChannel(Integer timeout) {
        SocketChannel result = null;
        try {
            result = SocketChannel.open();

            if (timeout != null) {
                result.socket().connect(socketAddress, timeout);
            } else {
                result.connect(socketAddress);
            }
            return result;
        } catch (Exception e) {
            if (result != null) {
                try {
                    result.close();
                } catch (IOException ignored) {
                }
            }
            throw new CommunicationException("Failed to connect to node " + this.toString(), e);
        }
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
        return "TarantoolNode{" +
                "socketAddress=" + socketAddress.getHostString() + ":" + socketAddress.getPort() +
                '}';
    }
}
