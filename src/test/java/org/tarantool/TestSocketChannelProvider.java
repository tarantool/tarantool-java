package org.tarantool;

import java.net.InetSocketAddress;
import java.nio.channels.SocketChannel;

/**
 * Socket channel provider to be used throughout the tests.
 */
public class TestSocketChannelProvider implements SocketChannelProvider {
    String host;
    int port;
    int restart_timeout;

    public TestSocketChannelProvider(String host, int port, int restart_timeout) {
        this.host = host;
        this.port = port;
        this.restart_timeout = restart_timeout;
    }

    @Override
    public SocketChannel get(int retryNumber, Throwable lastError) {
        long budget = System.currentTimeMillis() + restart_timeout;
        while (!Thread.currentThread().isInterrupted()) {
            try {
                return SocketChannel.open(new InetSocketAddress(host, port));
            } catch (Exception e) {
                if (budget < System.currentTimeMillis())
                    throw new RuntimeException(e);
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
}
