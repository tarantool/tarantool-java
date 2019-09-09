package org.tarantool;

import static java.net.StandardSocketOptions.SO_LINGER;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.channels.SocketChannel;

/**
 * Socket channel provider to be used throughout the tests.
 */
public class TestSocketChannelProvider implements SocketChannelProvider {
    String host;
    int port;
    int restartTimeout;
    int soLinger;

    public TestSocketChannelProvider(String host, int port, int restartTimeout) {
        this.host = host;
        this.port = port;
        this.restartTimeout = restartTimeout;
        this.soLinger = -1;
    }

    public TestSocketChannelProvider setSoLinger(int soLinger) {
        this.soLinger = soLinger;
        return this;
    }

    @Override
    public SocketChannel get(int retryNumber, Throwable lastError) {
        long budget = System.currentTimeMillis() + restartTimeout;
        while (!Thread.currentThread().isInterrupted()) {
            try {
                SocketChannel channel = SocketChannel.open();
                /*
                 * A value less then zero means disable lingering (it is a
                 * default behaviour).
                 */
                channel.setOption(SO_LINGER, soLinger);
                channel.connect(getAddress());
                return channel;
            } catch (Exception e) {
                if (budget < System.currentTimeMillis()) {
                    throw new RuntimeException(e);
                }
                try {
                    Thread.sleep(100);
                } catch (InterruptedException ex) {
                    // No-op.
                    Thread.currentThread().interrupt();
                }
            }
        }
        throw new RuntimeException(new InterruptedException());
    }

    @Override
    public SocketAddress getAddress() {
        return new InetSocketAddress(host, port);
    }
}
