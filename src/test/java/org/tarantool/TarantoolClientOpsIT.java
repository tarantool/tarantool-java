package org.tarantool;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.tarantool.TestAssertions.checkRawTupleResult;
import static org.tarantool.TestAssumptions.assumeMaximalServerVersion;
import static org.tarantool.TestAssumptions.assumeMinimalServerVersion;
import static org.tarantool.TestUtils.makeDefaultClientConfig;
import static org.tarantool.TestUtils.makeTestClient;
import static org.tarantool.TestUtils.openConnection;
import static org.tarantool.TestUtils.toLuaDelete;
import static org.tarantool.TestUtils.toLuaSelect;

import org.tarantool.util.ServerVersion;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.function.Executable;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.math.BigInteger;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Stream;

/**
 * Tests operations available in {@link TarantoolClientOps} interface.
 *
 * NOTE: Parametrized tests can be simplified after
 * https://github.com/junit-team/junit5/issues/878
 */
public class TarantoolClientOpsIT {

    private static final int RESTART_TIMEOUT = 2000;

    private static final String SPACE_NAME = "basic_test";
    private static final String MULTIPART_SPACE_NAME = "multipart_test";

    private int spaceId;
    private int multiPartSpaceId;

    private int pkIndexId;
    private int mpkIndexId;
    private int vidxIndexId;

    private static TarantoolTestHelper testHelper;

    private static final String[] SETUP_SCRIPT = new String[] {
        "box.schema.space.create('basic_test', { format = " +
            "{{name = 'id', type = 'integer'}," +
            " {name = 'val', type = 'string'} } })",

        "box.space.basic_test:create_index('pk', { type = 'TREE', parts = {'id'} } )",
        "box.space.basic_test:create_index('vidx', { type = 'TREE', unique = false, parts = {'val'} } )",

        "box.space.basic_test:replace{1, 'one'}",
        "box.space.basic_test:replace{2, 'two'}",
        "box.space.basic_test:replace{3, 'three'}",

        "box.schema.space.create('multipart_test', { format = " +
            "{{name = 'id1', type = 'integer'}," +
            " {name = 'id2', type = 'string'}," +
            " {name = 'val1', type = 'string'} } })",

        "box.space.multipart_test:create_index('pk', { type = 'TREE', parts = {'id1', 'id2'} })",
        "box.space.multipart_test:create_index('vidx', { type = 'TREE', unique = false, parts = {'val1'} })",

        "box.space.multipart_test:replace{1, 'one', 'o n e'}",
        "box.space.multipart_test:replace{2, 'two', 't w o'}",
        "box.space.multipart_test:replace{3, 'three', 't h r e e'}",

        "function echo(...) return ... end"
    };

    private static final String[] CLEAN_SCRIPT = new String[] {
        "box.space.basic_test and box.space.basic_test:drop()",
        "box.space.multipart_test and box.space.multipart_test:drop()"
    };

    @BeforeAll
    static void setupEnv() {
        testHelper = new TarantoolTestHelper("tnt-client-ops-it");
        testHelper.createInstance();
        testHelper.startInstance();
    }

    @AfterAll
    static void cleanupEnv() {
        testHelper.stopInstance();
    }

    @BeforeEach
    void setUpTest() {
        testHelper.executeLua(SETUP_SCRIPT);

        spaceId = testHelper.evaluate("box.space.basic_test.id");
        pkIndexId = testHelper.evaluate("box.space.basic_test.index.pk.id");
        vidxIndexId = testHelper.evaluate("box.space.basic_test.index.vidx.id");

        multiPartSpaceId = testHelper.evaluate("box.space.multipart_test.id");
        mpkIndexId = testHelper.evaluate("box.space.multipart_test.index.pk.id");
    }

    @AfterEach
    void tearDownTest() {
        testHelper.executeLua(CLEAN_SCRIPT);
    }

    static Stream<SyncOpsProvider> getClientOps() {
        return Stream.of(new ClientSyncOpsProvider(), new ConnectionSyncOpsProvider());
    }

    @ParameterizedTest
    @MethodSource("getClientOps")
    public void testSelectOne(SyncOpsProvider provider) {
        List<?> res = provider.getClientOps()
            .select(spaceId, pkIndexId, Collections.singletonList(1), 0, 1, Iterator.EQ);
        checkRawTupleResult(res, Arrays.asList(1, "one"));

        provider.close();
    }

    @ParameterizedTest
    @MethodSource("getClientOps")
    public void testSelectMany(SyncOpsProvider provider) {
        List<?> res = provider.getClientOps()
            .select(spaceId, pkIndexId, Collections.singletonList(10), 0, 10, Iterator.LT);

        assertNotNull(res);
        assertEquals(3, res.size());

        // Descending order.
        assertEquals(Arrays.asList(3, "three"), res.get(0));
        assertEquals(Arrays.asList(2, "two"), res.get(1));
        assertEquals(Arrays.asList(1, "one"), res.get(2));

        provider.close();
    }

    @ParameterizedTest
    @MethodSource("getClientOps")
    public void testSelectOffsetLimit(SyncOpsProvider provider) {
        List<?> res = provider.getClientOps()
            .select(spaceId, pkIndexId, Collections.singletonList(10), 1, 1, Iterator.LT);
        assertNotNull(res);
        assertEquals(1, res.size());

        assertEquals(Arrays.asList(2, "two"), res.get(0));

        provider.close();
    }

    @ParameterizedTest
    @MethodSource("getClientOps")
    public void testSelectUsingSecondaryIndex(SyncOpsProvider provider) {
        List<?> res = provider.getClientOps()
            .select(spaceId, vidxIndexId, Collections.singletonList("one"), 0, 1, Iterator.EQ);
        checkRawTupleResult(res, Arrays.asList(1, "one"));

        provider.close();
    }

    @ParameterizedTest
    @MethodSource("getClientOps")
    public void testInsertSimple(SyncOpsProvider provider) {
        List tup = Arrays.asList(100, "hundred");
        List<?> res = provider.getClientOps().insert(spaceId, tup);

        checkRawTupleResult(res, tup);

        // Check it actually was inserted.
        checkRawTupleResult(consoleSelect(SPACE_NAME, 100), tup);

        // Leave the database in a clean state.
        consoleDelete(SPACE_NAME, 100);

        provider.close();
    }

    private List<?> consoleDelete(String spaceName, Object key) {
        return testHelper.evaluate(toLuaDelete(spaceName, key));
    }

    private List<?> consoleSelect(String spaceName, Object key) {
        return testHelper.evaluate(toLuaSelect(spaceName, key));
    }

    @ParameterizedTest
    @MethodSource("getClientOps")
    public void testInsertBigInteger(SyncOpsProvider provider) {
        BigInteger id = BigInteger.valueOf(2).pow(64).subtract(BigInteger.ONE);

        List tup = Arrays.asList(id, "big");
        List<?> res = provider.getClientOps().insert(spaceId, tup);

        checkRawTupleResult(res, tup);

        // Check it actually was inserted.
        checkRawTupleResult(consoleSelect(SPACE_NAME, id), tup);

        // Leave the database in a clean state.
        consoleDelete(SPACE_NAME, id);

        provider.close();
    }

    @ParameterizedTest
    @MethodSource("getClientOps")
    public void testInsertMultiPart(SyncOpsProvider provider) {
        List tup = Arrays.asList(100, "hundred", "h u n d r e d");
        List<?> res = provider.getClientOps().insert(multiPartSpaceId, tup);

        checkRawTupleResult(res, tup);

        List<?> id = Arrays.asList(100, "hundred");

        // Check it actually was inserted.
        checkRawTupleResult(consoleSelect(MULTIPART_SPACE_NAME, id), tup);

        // Leave the database in a clean state.
        consoleDelete(MULTIPART_SPACE_NAME, id);

        provider.close();
    }

    @ParameterizedTest
    @MethodSource("getClientOps")
    public void testReplaceSimple(SyncOpsProvider provider) {
        checkReplace(
            provider.getClientOps(),
            SPACE_NAME,
            spaceId,
            Collections.singletonList(10),
            Arrays.asList(10, "10"),
            Arrays.asList(10, "ten")
        );

        provider.close();
    }

    @ParameterizedTest
    @MethodSource("getClientOps")
    public void testReplaceMultiPartKey(SyncOpsProvider provider) {
        checkReplace(
            provider.getClientOps(),
            MULTIPART_SPACE_NAME,
            multiPartSpaceId,
            Arrays.asList(10, "10"),
            Arrays.asList(10, "10", "10"),
            Arrays.asList(10, "10", "ten")
        );

        provider.close();
    }

    private void checkReplace(TarantoolClientOps<Integer, List<?>, Object, List<?>> clientOps,
                              String space,
                              int spaceId,
                              List key,
                              List createTuple,
                              List updateTuple) {
        List<?> res = clientOps.replace(spaceId, createTuple);
        checkRawTupleResult(res, createTuple);

        // Check it actually was created.
        checkRawTupleResult(consoleSelect(space, key), createTuple);

        // Update
        res = clientOps.replace(spaceId, updateTuple);
        checkRawTupleResult(res, updateTuple);

        // Check it actually was updated.
        checkRawTupleResult(consoleSelect(space, key), updateTuple);
    }

    @ParameterizedTest
    @MethodSource("getClientOps")
    public void testUpdateNonExistingHasNoEffect(SyncOpsProvider provider) {
        List op0 = Arrays.asList("=", 3, "trez");

        List res = provider.getClientOps().update(spaceId, Collections.singletonList(30), op0);

        assertNotNull(res);
        assertEquals(0, res.size());

        // Check it doesn't exist.
        res = provider.getClientOps()
            .select(spaceId, pkIndexId, Collections.singletonList(30), 0, 1, Iterator.EQ);
        assertNotNull(res);
        assertEquals(0, res.size());

        provider.close();
    }

    @ParameterizedTest
    @MethodSource("getClientOps")
    public void testUpdate(SyncOpsProvider provider) {
        checkUpdate(
            provider.getClientOps(),
            SPACE_NAME,
            spaceId,
            // key
            Collections.singletonList(30),
            // init tuple
            Arrays.asList(30, "30"),
            // expected tuple
            Arrays.asList(30, "thirty"),
            // operations
            Arrays.asList("!", 1, "thirty"),
            Arrays.asList("#", 2, 1)
        );

        provider.close();
    }

    @ParameterizedTest
    @MethodSource("getClientOps")
    public void testUpdateMultiPart(SyncOpsProvider provider) {
        checkUpdate(
            provider.getClientOps(),
            MULTIPART_SPACE_NAME,
            multiPartSpaceId,
            Arrays.asList(30, "30"),
            Arrays.asList(30, "30", "30"),
            Arrays.asList(30, "30", "thirty"),
            Arrays.asList("=", 2, "thirty")
        );

        provider.close();
    }

    private void checkUpdate(TarantoolClientOps<Integer, List<?>, Object, List<?>> clientOps,
                             String space,
                             int spaceId,
                             List key,
                             List initTuple,
                             List expectedTuple,
                             Object... ops) {
        // Try update non-existing key.
        List<?> res = clientOps.update(spaceId, key, ops);
        assertNotNull(res);
        assertEquals(0, res.size());

        // Check it still doesn't exists.
        assertEquals(Collections.emptyList(), consoleSelect(space, key));

        // Create the tuple.
        res = clientOps.insert(spaceId, initTuple);
        checkRawTupleResult(res, initTuple);

        // Apply the update operations.
        res = clientOps.update(spaceId, key, ops);
        checkRawTupleResult(res, expectedTuple);

        // Check that update was actually performed.
        checkRawTupleResult(consoleSelect(space, key), expectedTuple);
    }

    @ParameterizedTest
    @MethodSource("getClientOps")
    public void testUpsertSimple(SyncOpsProvider provider) {
        checkUpsert(
            provider.getClientOps(),
            SPACE_NAME,
            spaceId,
            Collections.singletonList(40),
            Arrays.asList(40, "40"),
            Arrays.asList(40, "fourty"),
            Arrays.asList("=", 1, "fourty")
        );

        provider.close();
    }

    @ParameterizedTest
    @MethodSource("getClientOps")
    public void testUpsertMultiPart(SyncOpsProvider provider) {
        checkUpsert(
            provider.getClientOps(),
            MULTIPART_SPACE_NAME,
            multiPartSpaceId,
            Arrays.asList(40, "40"),
            Arrays.asList(40, "40", "40"),
            Arrays.asList(40, "40", "fourty"),
            Arrays.asList("=", 2, "fourty")
        );

        provider.close();
    }

    private void checkUpsert(TarantoolClientOps<Integer, List<?>, Object, List<?>> clientOps,
                             String space,
                             int spaceId,
                             List key,
                             List defTuple,
                             List expectedTuple,
                             Object... ops) {
        // Check that key doesn't exist.
        assertEquals(Collections.emptyList(), consoleSelect(space, key));

        // Try upsert non-existing key.
        List<?> res = clientOps.upsert(spaceId, key, defTuple, ops);
        assertNotNull(res);
        assertEquals(0, res.size());

        // Check that default tuple was inserted.
        checkRawTupleResult(consoleSelect(space, key), defTuple);

        // Apply the operations.
        res = clientOps.upsert(spaceId, key, defTuple, ops);
        assertNotNull(res);
        assertEquals(0, res.size());

        // Check that update was actually performed.
        checkRawTupleResult(consoleSelect(space, key), expectedTuple);
    }

    @ParameterizedTest
    @MethodSource("getClientOps")
    public void testDeleteSimple(SyncOpsProvider provider) {
        checkDelete(
            provider.getClientOps(),
            SPACE_NAME,
            spaceId,
            Collections.singletonList(50),
            Arrays.asList(50, "fifty")
        );

        provider.close();
    }

    @ParameterizedTest
    @MethodSource("getClientOps")
    public void testDeleteMultiPartKey(SyncOpsProvider provider) {
        checkDelete(
            provider.getClientOps(),
            MULTIPART_SPACE_NAME,
            multiPartSpaceId,
            Arrays.asList(50, "50"),
            Arrays.asList(50, "50", "fifty")
        );

        provider.close();
    }

    private void checkDelete(TarantoolClientOps<Integer, List<?>, Object, List<?>> clientOps,
                             String space,
                             int spaceId,
                             List key,
                             List tuple) {
        // Check the key doesn't exists.
        assertEquals(Collections.emptyList(), consoleSelect(space, key));

        // Try to delete non-existing key.
        List<?> res = clientOps.delete(spaceId, key);
        assertNotNull(res);
        assertEquals(0, res.size());

        // Create tuple.
        res = clientOps.insert(spaceId, tuple);
        checkRawTupleResult(res, tuple);

        // Check the tuple was created.
        checkRawTupleResult(consoleSelect(space, key), tuple);

        // Delete it.
        res = clientOps.delete(spaceId, key);
        checkRawTupleResult(res, tuple);

        // Check it actually was deleted.
        assertEquals(Collections.emptyList(), consoleSelect(space, key));
    }

    @ParameterizedTest
    @MethodSource("getClientOps")
    public void testEval(SyncOpsProvider provider) {
        assertEquals(
            Collections.singletonList("true"),
            provider.getClientOps().eval("return echo(...)", "true")
        );

        provider.close();
    }

    @ParameterizedTest
    @MethodSource("getClientOps")
    public void testCall(SyncOpsProvider provider) {
        assertEquals(
            Collections.singletonList("true"),
            provider.getClientOps().call("echo", "true")
        );

        provider.close();
    }

    @ParameterizedTest
    @MethodSource("getClientOps")
    public void testPing(SyncOpsProvider provider) {
        provider.getClientOps().ping();
        provider.close();
    }

    @ParameterizedTest
    @MethodSource("getClientOps")
    public void testDeleteFromNonExistingSpace(SyncOpsProvider provider) {
        TarantoolException ex = assertThrows(TarantoolException.class, new Executable() {
            @Override
            public void execute() throws Throwable {
                provider.getClientOps().delete(5555, Collections.singletonList(2));
            }
        });
        assertEquals("Space '5555' does not exist", ex.getMessage());

        provider.close();
    }

    @ParameterizedTest
    @MethodSource("getClientOps")
    public void testSelectUnsupportedIterator(SyncOpsProvider provider) {
        TarantoolException ex = assertThrows(TarantoolException.class, new Executable() {
            @Override
            public void execute() throws Throwable {
                provider.getClientOps()
                    .select(spaceId, pkIndexId, Collections.singletonList(1), 0, 1, Iterator.OVERLAPS);
            }
        });
        assertEquals(
            "Index 'pk' (TREE) of space 'basic_test' (memtx) does not support requested iterator type",
            ex.getMessage()
        );

        provider.close();
    }

    @ParameterizedTest
    @MethodSource("getClientOps")
    public void testSelectNonExistingKey(SyncOpsProvider provider) {
        List<?> res = provider.getClientOps()
            .select(spaceId, pkIndexId, Collections.singletonList(5555), 0, 1, Iterator.EQ);

        assertNotNull(res);
        assertEquals(0, res.size());

        provider.close();
    }

    @ParameterizedTest
    @MethodSource("getClientOps")
    public void testSelectFromNonExistingIndex(SyncOpsProvider provider) {
        TarantoolException ex = assertThrows(TarantoolException.class, new Executable() {
            @Override
            public void execute() throws Throwable {
                provider.getClientOps().select(spaceId, 5555, Collections.singletonList(2), 0, 1, Iterator.EQ);
            }
        });
        assertEquals("No index #5555 is defined in space 'basic_test'", ex.getMessage());

        provider.close();
    }

    @ParameterizedTest
    @MethodSource("getClientOps")
    public void testSelectFromNonExistingSpace(SyncOpsProvider provider) {
        TarantoolException ex = assertThrows(TarantoolException.class, new Executable() {
            @Override
            public void execute() throws Throwable {
                provider.getClientOps()
                    .select(5555, 0, Collections.singletonList(5555), 0, 1, Iterator.EQ);
            }
        });
        assertEquals("Space '5555' does not exist", ex.getMessage());

        provider.close();
    }

    @ParameterizedTest
    @MethodSource("getClientOps")
    public void testInsertDuplicateKey(SyncOpsProvider provider) {
        final List tup = Arrays.asList(1, "uno");
        TarantoolException ex = assertThrows(
            TarantoolException.class,
            () -> provider.getClientOps().insert(spaceId, tup)
        );
        assertEquals("Duplicate key exists in unique index 'pk' in space 'basic_test'", ex.getMessage());

        // Check the tuple stayed intact.
        checkRawTupleResult(consoleSelect(SPACE_NAME, 1), Arrays.asList(1, "one"));

        provider.close();
    }

    @ParameterizedTest
    @MethodSource("getClientOps")
    public void testInsertToNonExistingSpace(SyncOpsProvider provider) {
        TarantoolException ex = assertThrows(TarantoolException.class, new Executable() {
            @Override
            public void execute() throws Throwable {
                provider.getClientOps().insert(5555, Arrays.asList(1, "one"));
            }
        });
        assertEquals("Space '5555' does not exist", ex.getMessage());

        provider.close();
    }

    @ParameterizedTest
    @MethodSource("getClientOps")
    void testInsertInvalidData(SyncOpsProvider provider) {
        // Invalid types.
        TarantoolException ex = assertThrows(
            TarantoolException.class,
            () -> provider.getClientOps().insert(spaceId, Arrays.asList("one", 1))
        );
        assertEquals("Tuple field 1 type does not match one required by operation: expected integer", ex.getMessage());

        provider.close();
    }

    @ParameterizedTest
    @MethodSource("getClientOps")
    public void testInsertInvalidTupleSize2xVersion(SyncOpsProvider provider) {
        assumeMinimalServerVersion(testHelper.getInstanceVersion(), ServerVersion.V_2_1);

        // Invalid tuple size.
        TarantoolException ex = assertThrows(
            TarantoolException.class,
            () -> provider.getClientOps().insert(spaceId, Collections.singletonList(101))
        );
        assertEquals("Tuple field 2 required by space format is missing", ex.getMessage());

        provider.close();
    }

    @ParameterizedTest
    @MethodSource("getClientOps")
    public void testInsertInvalidTupleSize1xVersion(SyncOpsProvider provider) {
        assumeMaximalServerVersion(testHelper.getInstanceVersion(), ServerVersion.V_1_10);

        // Invalid tuple size.
        TarantoolException ex = assertThrows(
            TarantoolException.class,
            () -> provider.getClientOps().insert(spaceId, Collections.singletonList(101))
        );
        assertEquals("Tuple field count 1 is less than required by space format or defined indexes " +
            "(expected at least 2)", ex.getMessage()
        );

        provider.close();
    }

    private static TarantoolConnection makeConnection() {
        return openConnection(
            TarantoolTestHelper.HOST,
            TarantoolTestHelper.PORT,
            TarantoolTestHelper.USERNAME,
            TarantoolTestHelper.PASSWORD
        );
    }

    private interface SyncOpsProvider {
        TarantoolClientOps<Integer, List<?>, Object, List<?>> getClientOps();

        void close();
    }

    private static class ClientSyncOpsProvider implements SyncOpsProvider {

        private TarantoolClient client = makeTestClient(makeDefaultClientConfig(), RESTART_TIMEOUT);

        @Override
        public TarantoolClientOps<Integer, List<?>, Object, List<?>> getClientOps() {
            return client.syncOps();
        }

        @Override
        public void close() {
            client.close();
        }

    }

    private static class ConnectionSyncOpsProvider implements SyncOpsProvider {

        private TarantoolConnection connection = makeConnection();

        @Override
        public TarantoolClientOps<Integer, List<?>, Object, List<?>> getClientOps() {
            return connection;
        }

        @Override
        public void close() {
            connection.close();
        }

    }

}
