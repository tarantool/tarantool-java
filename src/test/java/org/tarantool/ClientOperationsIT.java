package org.tarantool;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Tests for synchronous operations of {@link TarantoolClientImpl} class.
 *
 * @see TarantoolClientOpsIT
 */
public class ClientOperationsIT {

    private static TarantoolTestHelper testHelper;

    private TarantoolClient client;

    @BeforeAll
    public static void setUpEnv() {
        testHelper = new TarantoolTestHelper("client-ops-it");
        testHelper.createInstance();
        testHelper.startInstance();
    }

    @AfterAll
    public static void tearDownEnv() {
        testHelper.stopInstance();
    }

    @BeforeEach
    public void setUp() {
        client = TestUtils.makeTestClient(TestUtils.makeDefaultClientConfig(), 2000);
    }

    @AfterEach
    public void tearDown() {
        client.close();
    }

    @Test
    public void testClose() {
        IllegalStateException e = assertThrows(IllegalStateException.class, () -> client.syncOps().close());
        assertEquals(e.getMessage(), "You should close TarantoolClient instead.");
    }

}
