package org.tarantool;


import java.nio.channels.SocketChannel;

public interface SocketChannelProvider {

    void connect();

    /**
     * Provides socket channel to init restore connection.
     * You could change hosts on fail and sleep between retries in this method
     * @return the result of SocketChannel open(SocketAddress remote) call
     */
    SocketChannel getNext();

    SocketChannel getChannel();
}
