package org.tarantool;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.net.SocketException;

/**
 * {@link TarantoolConnection} specific test cases.
 *
 * @see TarantoolSQLOpsIT
 * @see TarantoolClientOpsIT
 */
public class ConnectionIT {

    private static TarantoolTestHelper testHelper;

    private TarantoolConnection conn;

    @BeforeAll
    static void setupEnv() {
        testHelper = new TarantoolTestHelper("tnt-connection-it");
        testHelper.createInstance();
        testHelper.startInstance();
    }

    @AfterAll
    static void cleanupEnv() {
        testHelper.stopInstance();
    }

    @BeforeEach
    public void setup() {
        conn = TestUtils.openConnection(
            TarantoolTestHelper.HOST,
            TarantoolTestHelper.PORT,
            TarantoolTestHelper.USERNAME,
            TarantoolTestHelper.PASSWORD
        );
    }

    @AfterEach
    public void tearDown() {
        conn.close();
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
