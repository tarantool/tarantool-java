package org.tarantool;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.tarantool.TestAssertions.checkRawTupleResult;
import static org.tarantool.TestAssumptions.assumeMinimalServerVersion;

import org.tarantool.schema.TarantoolIndexNotFoundException;
import org.tarantool.schema.TarantoolSpaceNotFoundException;
import org.tarantool.util.ServerVersion;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

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
        testHelper.executeLua(
            "box.schema.space.create('basic_test', { format = " +
                "{{name = 'id', type = 'integer'}," +
                " {name = 'val', type = 'string'} } })",
            "box.space.basic_test:create_index('pk', { type = 'TREE', parts = {'id'} } )"
        );
        client = TestUtils.makeTestClient(TestUtils.makeDefaultClientConfig(), 2000);
    }

    @AfterEach
    public void tearDown() {
        testHelper.executeLua("box.space.basic_test and box.space.basic_test:drop()");
        client.close();
    }

    @Test
    public void testClose() {
        IllegalStateException e = assertThrows(IllegalStateException.class, () -> client.syncOps().close());
        assertEquals(e.getMessage(), "You should close TarantoolClient instead.");
    }

    @Test
    void testStringSelect() {
        testHelper.executeLua("box.space.basic_test:insert{1, 'one'}");
        List<?> result = client.syncOps()
            .select("basic_test", "pk", Collections.singletonList(1), 0, 1, Iterator.EQ);

        assertEquals(Collections.singletonList(Arrays.asList(1, "one")), result);
    }

    @Test
    void testStringInsert() {
        client.syncOps().insert("basic_test", Arrays.asList(1, "one"));
        client.syncOps().insert("basic_test", Arrays.asList(10, "ten"));

        checkRawTupleResult(consoleSelect("basic_test", 1), Arrays.asList(1, "one"));
        checkRawTupleResult(consoleSelect("basic_test", 10), Arrays.asList(10, "ten"));
    }

    @Test
    void testStringReplace() {
        testHelper.executeLua("box.space.basic_test:insert{1, '1'}");
        testHelper.executeLua("box.space.basic_test:insert{10, '10'}");

        client.syncOps().replace("basic_test", Arrays.asList(1, "one"));
        client.syncOps().replace("basic_test", Arrays.asList(10, "ten"));

        checkRawTupleResult(consoleSelect("basic_test", 1), Arrays.asList(1, "one"));
        checkRawTupleResult(consoleSelect("basic_test", 10), Arrays.asList(10, "ten"));
    }

    @Test
    void testStringDelete() {
        testHelper.executeLua("box.space.basic_test:insert{1, '1'}");
        testHelper.executeLua("box.space.basic_test:insert{10, '10'}");
        testHelper.executeLua("box.space.basic_test:insert{20, '20'}");

        client.syncOps().delete("basic_test", Collections.singletonList(1));
        client.syncOps().delete("basic_test", Collections.singletonList(20));

        assertEquals(Collections.emptyList(), consoleSelect("basic_test", 1));
        checkRawTupleResult(consoleSelect("basic_test", 10), Arrays.asList(10, "10"));
        assertEquals(Collections.emptyList(), consoleSelect("basic_test", 20));
    }

    @Test
    void testStringUpdate() {
        testHelper.executeLua("box.space.basic_test:insert{1, '1'}");
        testHelper.executeLua("box.space.basic_test:insert{10, '10'}");

        TarantoolClientOps<Integer, List<?>, Object, List<?>> clientOps = client.syncOps();
        clientOps.update("basic_test", Collections.singletonList(1), Arrays.asList("=", 1, "one"));
        clientOps.update("basic_test", Collections.singletonList(2), Arrays.asList("=", 1, "two"));
        clientOps.update("basic_test", Collections.singletonList(10), Arrays.asList("=", 1, "ten"));

        checkRawTupleResult(consoleSelect("basic_test", 1), Arrays.asList(1, "one"));
        assertEquals(Collections.emptyList(), consoleSelect("basic_test", 2));
        checkRawTupleResult(consoleSelect("basic_test", 10), Arrays.asList(10, "ten"));
    }

    @Test
    void testStringUpsert() {
        testHelper.executeLua("box.space.basic_test:insert{1, '1'}");
        testHelper.executeLua("box.space.basic_test:insert{10, '10'}");

        TarantoolClientOps<Integer, List<?>, Object, List<?>> ops = client.syncOps();
        ops.upsert(
            "basic_test", Collections.singletonList(1),
            Arrays.asList(1, "001"), Arrays.asList("=", 1, "one")
        );
        ops.upsert(
            "basic_test", Collections.singletonList(2),
            Arrays.asList(2, "002"), Arrays.asList("=", 1, "two")
        );
        ops.upsert(
            "basic_test", Collections.singletonList(10),
            Arrays.asList(10, "010"), Arrays.asList("=", 1, "ten")
        );

        checkRawTupleResult(consoleSelect("basic_test", 1), Arrays.asList(1, "one"));
        checkRawTupleResult(consoleSelect("basic_test", 2), Arrays.asList(2, "002"));
        checkRawTupleResult(consoleSelect("basic_test", 10), Arrays.asList(10, "ten"));
    }

    @Test
    void testStringMultipleIndirectChanges() {
        testHelper.executeLua("box.space.basic_test:insert{1, 'one'}");
        List<?> result = client.syncOps().select("basic_test", "pk", Collections.singletonList(1), 0, 1, Iterator.EQ);
        assertEquals(Collections.singletonList(Arrays.asList(1, "one")), result);

        testHelper.executeLua("box.space.basic_test and box.space.basic_test:drop()");
        testHelper.executeLua(
            "box.schema.space.create('basic_test', { format = " +
                "{{name = 'id', type = 'integer'}," +
                " {name = 'val', type = 'string'} } })",

            "box.space.basic_test:create_index('pk', { type = 'TREE', parts = {'id'} } )"
        );
        testHelper.executeLua("box.space.basic_test:insert{2, 'two'}");
        result = client.syncOps().select("basic_test", "pk", Collections.singletonList(2), 0, 1, Iterator.EQ);
        assertEquals(Collections.singletonList(Arrays.asList(2, "two")), result);

        testHelper.executeLua("box.space.basic_test and box.space.basic_test:drop()");
        testHelper.executeLua(
            "box.schema.space.create('basic_test', { format = " +
                "{{name = 'id', type = 'integer'}," +
                " {name = 'val', type = 'string'} } })",

            "box.space.basic_test:create_index('pk', { type = 'TREE', parts = {'id'} } )"
        );
        testHelper.executeLua("box.space.basic_test:insert{3, 'three'}");
        result = client.syncOps().select("basic_test", "pk", Collections.singletonList(3), 0, 1, Iterator.EQ);
        assertEquals(Collections.singletonList(Arrays.asList(3, "three")), result);
    }

    @Test
    void testUnknownSpace() {
        TarantoolClientOps<Integer, List<?>, Object, List<?>> clientOps = client.syncOps();
        Exception error = assertThrows(
            Exception.class,
            () -> clientOps.select("base_test_unknown", "pk", Collections.singletonList(12), 0, 1, Iterator.EQ)
        );

        assertTrue(error.getCause() instanceof TarantoolSpaceNotFoundException);
    }

    @Test
    void testUnknownSpaceIndex() {
        TarantoolClientOps<Integer, List<?>, Object, List<?>> clientOps = client.syncOps();
        Exception error = assertThrows(
            Exception.class,
            () -> clientOps.select("basic_test", "pk_unknown", Collections.singletonList(12), 0, 1, Iterator.EQ)
        );

        assertTrue(error.getCause() instanceof TarantoolIndexNotFoundException);
    }

    @Test
    void testCreateSpaceAfterFailedRequest() {
        TarantoolClientOps<Integer, List<?>, Object, List<?>> clientOps = client.syncOps();
        Exception error = assertThrows(
            Exception.class,
            () -> clientOps
                .select("base_test_unknown", "pk", Collections.emptyList(), 0, 10, Iterator.ALL)
        );
        assertTrue(error.getCause() instanceof TarantoolSpaceNotFoundException);

        testHelper.executeLua(
            "box.schema.space.create('base_test_unknown', { format = { { name = 'id', type = 'integer' } } })",
            "box.space.base_test_unknown:create_index('pk', { type = 'TREE', parts = {'id'} } )",
            "box.space.base_test_unknown:insert{ 5 }"
        );
        List<?> result = clientOps
            .select("base_test_unknown", "pk", Collections.emptyList(), 0, 10, Iterator.ALL);
        assertEquals(Collections.singletonList(5), result.get(0));

        error = assertThrows(
            Exception.class,
            () -> clientOps
                .select("base_test_unknown1", "pk", Collections.emptyList(), 0, 10, Iterator.ALL)
        );
        assertTrue(error.getCause() instanceof TarantoolSpaceNotFoundException);

        testHelper.executeLua("box.space.base_test_unknown:drop()");
    }

    @Test
    void testNamedFieldUpdate() {
        assumeMinimalServerVersion(testHelper.getInstanceVersion(), ServerVersion.V_2_3);
        testHelper.executeLua("box.space.basic_test:insert{1, '1'}");
        testHelper.executeLua("box.space.basic_test:insert{10, '10'}");

        TarantoolClientOps<Integer, List<?>, Object, List<?>> clientOps = client.syncOps();
        clientOps.update("basic_test", Collections.singletonList(1), Arrays.asList("=", "val", "un"));
        clientOps.update("basic_test", Collections.singletonList(2), Arrays.asList("=", "val", "deux"));
        clientOps.update("basic_test", Collections.singletonList(10), Arrays.asList("=", "val", "dix"));

        checkRawTupleResult(consoleSelect("basic_test", 1), Arrays.asList(1, "un"));
        assertEquals(Collections.emptyList(), consoleSelect("basic_test", 2));
        checkRawTupleResult(consoleSelect("basic_test", 10), Arrays.asList(10, "dix"));
    }

    @Test
    void testWrongNamedFieldUpdate() {
        assumeMinimalServerVersion(testHelper.getInstanceVersion(), ServerVersion.V_2_3);
        testHelper.executeLua("box.space.basic_test:insert{1, '1'}");

        TarantoolClientOps<Integer, List<?>, Object, List<?>> clientOps = client.syncOps();
        TarantoolException exception = assertThrows(
            TarantoolException.class,
            () -> clientOps.update("basic_test", Collections.singletonList(1), Arrays.asList("=", "wrong", "un"))
        );
        assertEquals(exception.getMessage(), "Field 'wrong' was not found in the tuple");
    }

    @Test
    void testNamedFieldUpsert() {
        assumeMinimalServerVersion(testHelper.getInstanceVersion(), ServerVersion.V_2_3);
        testHelper.executeLua("box.space.basic_test:insert{1, '1'}");
        testHelper.executeLua("box.space.basic_test:insert{10, '10'}");

        TarantoolClientOps<Integer, List<?>, Object, List<?>> ops = client.syncOps();
        ops.upsert(
            "basic_test", Collections.singletonList(1),
            Arrays.asList(1, "un"), Arrays.asList("=", "val", "one")
        );
        ops.upsert(
            "basic_test", Collections.singletonList(2),
            Arrays.asList(2, "deux"), Arrays.asList("=", "val", "two")
        );
        ops.upsert(
            "basic_test", Collections.singletonList(10),
            Arrays.asList(10, "dix"), Arrays.asList("=", "val", "ten")
        );

        checkRawTupleResult(consoleSelect("basic_test", 1), Arrays.asList(1, "one"));
        checkRawTupleResult(consoleSelect("basic_test", 2), Arrays.asList(2, "deux"));
        checkRawTupleResult(consoleSelect("basic_test", 10), Arrays.asList(10, "ten"));
    }

    @Test
    void testWrongNamedFieldUpsert() {
        assumeMinimalServerVersion(testHelper.getInstanceVersion(), ServerVersion.V_2_3);
        testHelper.executeLua("box.space.basic_test:insert{1, '1'}");

        TarantoolClientOps<Integer, List<?>, Object, List<?>> ops = client.syncOps();
        TarantoolException exception = assertThrows(
            TarantoolException.class,
            () -> {
                ops.upsert(
                    "basic_test", Collections.singletonList(1),
                    Arrays.asList(1, "un"), Arrays.asList("=", "bad_field", "one")
                );
            }
        );
        assertEquals(exception.getMessage(), "Field 'bad_field' was not found in the tuple");
    }

    @Test
    void testUpdateNamedFieldsOperations() {
        assumeMinimalServerVersion(testHelper.getInstanceVersion(), ServerVersion.V_2_3);
        testHelper.executeLua(
            "box.schema.space.create('animals', { format = " +
                "{ {name = 'id', type = 'integer'}," +
                "  {name = 'name', type = 'string'}," +
                "  {name = 'mph', type = 'integer'}," +
                "  {name = 'habitat', type = 'string', is_nullable = true} } })",
            "box.space.animals:create_index('pk', { type = 'TREE', parts = {'id'} } )",

            "box.space.animals:insert{1, 'Golden eagle', 200}",
            "box.space.animals:insert{2, 'Cheetah', 75}",
            "box.space.animals:insert{3, 'lion', 30, 'Africa'}"
        );

        TarantoolClientOps<Integer, List<?>, Object, List<?>> ops = client.syncOps();
        ops.update("animals", Collections.singletonList(1), Arrays.asList("!", "habitat", "North America"));
        ops.update("animals", Collections.singletonList(3), Arrays.asList("=", "name", "Lion"));
        ops.update("animals", Collections.singletonList(3), Arrays.asList("+", "mph", 20));
        ops.update("animals", Collections.singletonList(3), Arrays.asList("#", "habitat", 1));
        ops.upsert(
            "animals", Collections.singletonList(4),
            Arrays.asList(4, "Swordfish", 60), Arrays.asList("=", "mph", 60)
        );

        checkRawTupleResult(consoleSelect("animals", 1), Arrays.asList(1, "Golden eagle", 200, "North America"));
        checkRawTupleResult(consoleSelect("animals", 2), Arrays.asList(2, "Cheetah", 75));
        checkRawTupleResult(consoleSelect("animals", 3), Arrays.asList(3, "Lion", 50));
        checkRawTupleResult(consoleSelect("animals", 4), Arrays.asList(4, "Swordfish", 60));

        testHelper.executeLua("box.space.animals:drop()");
    }

    private List<?> consoleSelect(String spaceName, Object key) {
        return testHelper.evaluate(TestUtils.toLuaSelect(spaceName, key));
    }

}
