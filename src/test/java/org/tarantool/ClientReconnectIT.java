package org.tarantool;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;

import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ClientReconnectIT extends AbstractTarantoolConnectorIT {
    private static final String INSTANCE_NAME = "jdk-testing";
    private TarantoolClient client;

    @BeforeEach
    public void setup() {
        client = makeClient();
    }

    @AfterEach
    public void tearDown() {
        client.close();

        // Re-open console for cleanupEnv() to work.
        console.close();
        console = openConsole();
    }

    @Test
    public void testReconnect() throws Exception {
        client.syncOps().ping();

        stopTarantool(INSTANCE_NAME);

        Exception e = assertThrows(Exception.class, new Executable() {
            @Override
            public void execute() {
                client.syncOps().ping();
            }
        });

        assertTrue(CommunicationException.class.isAssignableFrom(e.getClass()) ||
            IllegalStateException.class.isAssignableFrom(e.getClass()));

        assertNotNull(((TarantoolClientImpl) client).getThumbstone());

        assertFalse(client.isAlive());

        startTarantool(INSTANCE_NAME);

        assertTrue(client.waitAlive(TIMEOUT, TimeUnit.MILLISECONDS));

        client.syncOps().ping();
    }
}
