package org.tarantool;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

/**
 * Test operations of {@link TarantoolConnection} class.
 *
 * Actual tests reside in base class.
 */
public class ConnectionIT extends AbstractTarantoolOpsIT {
    private TarantoolConnection conn;

    @BeforeEach
    public void setup() {
        conn = openConnection();
    }

    @AfterEach
    public void tearDown() {
        conn.close();
    }

    @Override
    protected TarantoolClientOps<Integer, List<?>, Object, List<?>> getOps() {
        return conn;
    }

    @Test
    public void testClose() {
        conn.close();
    }
}
