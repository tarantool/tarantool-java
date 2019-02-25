package org.tarantool;


import org.tarantool.server.TarantoolBinaryPackage;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;

public interface SocketChannelProvider {

    void connect() throws IOException;

    /**
     * Provides socket channel to init restore connection.
     * You could change hosts on fail and sleep between retries in this method
     * @return the result of SocketChannel open(SocketAddress remote) call
     */
    SocketChannel getNext();

    SocketChannel getChannel();

    TarantoolBinaryPackage readPackage() throws IOException;

    void writeBuffer(ByteBuffer byteBuffer) throws IOException;
}
