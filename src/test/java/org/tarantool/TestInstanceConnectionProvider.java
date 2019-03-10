package org.tarantool;

import org.tarantool.server.TarantoolInstanceConnection;

import java.io.IOException;

public class TestInstanceConnectionProvider extends SingleInstanceConnectionProvider {
    private final long restartTimeout;

    public TestInstanceConnectionProvider(String address, String username, String password, long restartTimeout1) {
        super(address, username, password);

        this.restartTimeout = restartTimeout1;
    }

    @Override
    public TarantoolInstanceConnection connect() throws IOException {

        long budget = System.currentTimeMillis() + restartTimeout;
        while (!Thread.currentThread().isInterrupted()) {
            try {
                return super.connect();
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
