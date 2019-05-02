package org.tarantool;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.net.SocketException;
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
        conn = TestUtils.openConnection(host, port, username, password);
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
        assertTrue(conn.isClosed());
    }

    @Test
    void testGetSoTimeout() throws SocketException {
        assertEquals(0, conn.getSocketTimeout());
    }

    @Test
    void testSetSoTimeout() throws SocketException {
        conn.setSocketTimeout(2000);
        assertEquals(2000, conn.getSocketTimeout());
    }

}
