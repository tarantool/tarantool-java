package org.tarantool;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;

import java.util.List;

/**
 * Tests for synchronous operations of {@link TarantoolClientImpl} class.
 *
 * Actual tests reside in base class.
 */
public class ClientOperationsIT extends AbstractTarantoolOpsIT {
    private TarantoolClient client;

    @BeforeEach
    public void setup() {
        client = makeClient();
    }

    @AfterEach
    public void tearDown() {
        client.close();
    }

    @Override
    protected TarantoolClientOps<Integer, List<?>, Object, List<?>> getOps() {
        return client.syncOps();
    }

    @Test
    public void testClose() {
        IllegalStateException e = assertThrows(IllegalStateException.class, new Executable() {
            @Override
            public void execute() throws Throwable {
                getOps().close();
            }
        });
        assertEquals(e.getMessage(), "You should close TarantoolClient instead.");
    }
}
