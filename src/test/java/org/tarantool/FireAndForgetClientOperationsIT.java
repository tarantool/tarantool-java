package org.tarantool;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.tarantool.TestAssertions.checkRawTupleResult;
import static org.tarantool.TestUtils.makeDefaultClientConfig;
import static org.tarantool.TestUtils.makeTestClient;
import static org.tarantool.TestUtils.toLuaSelect;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Test "fire & forget" operations available in {@link TarantoolClientImpl} class.
 */
public class FireAndForgetClientOperationsIT {

    private static final int RESTART_TIMEOUT = 2000;

    private static TarantoolTestHelper testHelper;

    private static final String SPACE_NAME = "basic_test";
    private static final String[] SETUP_SCRIPT = new String[] {
        "box.schema.space.create('basic_test', { format = " +
            "{{name = 'id', type = 'integer'}," +
            " {name = 'val', type = 'string'} } })",

        "box.space.basic_test:create_index('pk', { type = 'TREE', parts = {'id'} } )",
    };

    private static final String[] CLEAN_SCRIPT = new String[] {
        "box.space.basic_test and box.space.basic_test:drop()",
    };

    private TarantoolClient client;
    private int spaceId;

    @BeforeAll
    static void setupEnv() {
        testHelper = new TarantoolTestHelper("fire-and-forget-ops-it");
        testHelper.createInstance();
        testHelper.startInstance();
    }

    @AfterAll
    static void cleanupEnv() {
        testHelper.stopInstance();
    }

    @BeforeEach
    public void setup() {
        testHelper.executeLua(SETUP_SCRIPT);

        spaceId = testHelper.evaluate("box.space.basic_test.id");
        client = makeTestClient(makeDefaultClientConfig(), RESTART_TIMEOUT);
    }

    @AfterEach
    public void tearDown() {
        client.close();
        testHelper.executeLua(CLEAN_SCRIPT);
    }

    @Test
    public void testPing() {
        // Half-ping actually.
        client.fireAndForgetOps().ping();
    }

    @Test
    public void testClose() {
        IllegalStateException e = assertThrows(IllegalStateException.class, new Executable() {
            @Override
            public void execute() throws Throwable {
                client.fireAndForgetOps().close();
            }
        });
        assertEquals(e.getMessage(), "You should close TarantoolClient instead.");
    }

    @Test
    public void testFireAndForgetOperations() {
        testHelper.executeLua(
            "box.space.basic_test:insert{1, '1'}",
            "box.space.basic_test:insert{5, '5'}",
            "box.space.basic_test:insert{10, '10'}",
            "box.space.basic_test:insert{20, '20'}",
            "box.space.basic_test:insert{30, '30'}"
        );

        TarantoolClientOps<Integer, List<?>, Object, Long> ffOps = client.fireAndForgetOps();

        Set<Long> syncIds = new HashSet<>();

        syncIds.add(ffOps.delete(spaceId, Collections.singletonList(1)));

        syncIds.add(ffOps.insert(spaceId, Arrays.asList(2, "2")));

        syncIds.add(ffOps.replace(spaceId, Arrays.asList(3, "3")));
        syncIds.add(ffOps.replace(spaceId, Arrays.asList(5, "five")));

        syncIds.add(ffOps.update(spaceId, Collections.singletonList(10), Arrays.asList("=", 1, "ten")));

        syncIds.add(
            ffOps.upsert(
                spaceId,
                Collections.singletonList(20),
                Arrays.asList(20, "twenty"),
                Arrays.asList("=", 1, "twenty")
            )
        );

        syncIds.add(
            ffOps.upsert(
                spaceId,
                Collections.singletonList(25),
                Arrays.asList(25, "25"),
                Arrays.asList("=", 1, "twenty five")
            )
        );

        syncIds.add(ffOps.call("box.space.basic_test:delete", Collections.singletonList(30)));

        // Check the syncs.
        assertFalse(syncIds.contains(0L));
        assertEquals(8, syncIds.size());

        // The reply for synchronous ping will
        // indicate to us that previous fire & forget operations are completed.
        client.syncOps().ping();

        // Check the effects
        assertEquals(Collections.emptyList(), consoleSelect(SPACE_NAME, 1));
        checkRawTupleResult(consoleSelect(SPACE_NAME, 2), Arrays.asList(2, "2"));
        checkRawTupleResult(consoleSelect(SPACE_NAME, 3), Arrays.asList(3, "3"));
        checkRawTupleResult(consoleSelect(SPACE_NAME, 5), Arrays.asList(5, "five"));
        checkRawTupleResult(consoleSelect(SPACE_NAME, 10), Arrays.asList(10, "ten"));
        checkRawTupleResult(consoleSelect(SPACE_NAME, 20), Arrays.asList(20, "twenty"));
        checkRawTupleResult(consoleSelect(SPACE_NAME, 25), Arrays.asList(25, "25"));
        assertEquals(Collections.emptyList(), consoleSelect(SPACE_NAME, 30));
    }

    @Test
    public void testFireAndForgetStringOperations() {
        testHelper.executeLua(
            "box.space.basic_test:insert{2, '2'}",
            "box.space.basic_test:insert{20, '20'}",
            "box.space.basic_test:insert{200, '200'}",
            "box.space.basic_test:insert{2000, '2000'}"
        );

        TarantoolClientOps<Integer, List<?>, Object, Long> ffOps = client.fireAndForgetOps();
        Set<Long> syncIds = new HashSet<>();

        syncIds.add(ffOps.delete(SPACE_NAME, Collections.singletonList(2)));
        syncIds.add(ffOps.insert(SPACE_NAME, Arrays.asList(3, "3")));
        syncIds.add(ffOps.replace(spaceId, Arrays.asList(2000, "2k")));
        syncIds.add(ffOps.replace(spaceId, Arrays.asList(3000, "3k")));
        syncIds.add(ffOps.update(SPACE_NAME, Collections.singletonList(20), Arrays.asList("=", 1, "twenty")));
        syncIds.add(
            ffOps.upsert(
                SPACE_NAME,
                Collections.singletonList(200),
                Arrays.asList(200, "200"),
                Arrays.asList("=", 1, "two hundred")
            )
        );
        syncIds.add(
            ffOps.upsert(
                SPACE_NAME,
                Collections.singletonList(400),
                Arrays.asList(400, "400"),
                Arrays.asList("=", 1, "four hundred")
            )
        );

        // Check the syncs.
        assertFalse(syncIds.contains(0L));
        assertEquals(7, syncIds.size());

        // The reply for synchronous ping will
        // indicate to us that previous fire & forget operations are completed.
        client.syncOps().ping();

        // Check the effects
        assertEquals(consoleSelect(SPACE_NAME, 2), Collections.emptyList());
        checkRawTupleResult(consoleSelect(SPACE_NAME, 3), Arrays.asList(3, "3"));
        checkRawTupleResult(consoleSelect(SPACE_NAME, 20), Arrays.asList(20, "twenty"));
        checkRawTupleResult(consoleSelect(SPACE_NAME, 200), Arrays.asList(200, "two hundred"));
        checkRawTupleResult(consoleSelect(SPACE_NAME, 400), Arrays.asList(400, "400"));
        checkRawTupleResult(consoleSelect(SPACE_NAME, 2000), Arrays.asList(2000, "2k"));
        checkRawTupleResult(consoleSelect(SPACE_NAME, 3000), Arrays.asList(3000, "3k"));
    }

    private List<?> consoleSelect(String spaceName, Object key) {
        return testHelper.evaluate(toLuaSelect(spaceName, key));
    }

}
