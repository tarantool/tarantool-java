package org.tarantool;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.tarantool.TestAssertions.checkRawTupleResult;

import org.tarantool.schema.TarantoolIndexNotFoundException;
import org.tarantool.schema.TarantoolSpaceNotFoundException;

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

        checkRawTupleResult(consoleSelect(1), Arrays.asList(1, "one"));
        checkRawTupleResult(consoleSelect(10), Arrays.asList(10, "ten"));
    }

    @Test
    void testStringReplace() {
        testHelper.executeLua("box.space.basic_test:insert{1, '1'}");
        testHelper.executeLua("box.space.basic_test:insert{10, '10'}");

        client.syncOps().replace("basic_test", Arrays.asList(1, "one"));
        client.syncOps().replace("basic_test", Arrays.asList(10, "ten"));

        checkRawTupleResult(consoleSelect(1), Arrays.asList(1, "one"));
        checkRawTupleResult(consoleSelect(10), Arrays.asList(10, "ten"));
    }

    @Test
    void testStringDelete() {
        testHelper.executeLua("box.space.basic_test:insert{1, '1'}");
        testHelper.executeLua("box.space.basic_test:insert{10, '10'}");
        testHelper.executeLua("box.space.basic_test:insert{20, '20'}");

        client.syncOps().delete("basic_test", Collections.singletonList(1));
        client.syncOps().delete("basic_test", Collections.singletonList(20));

        assertEquals(Collections.emptyList(), consoleSelect(1));
        checkRawTupleResult(consoleSelect(10), Arrays.asList(10, "10"));
        assertEquals(Collections.emptyList(), consoleSelect(20));
    }

    @Test
    void testStringUpdate() {
        testHelper.executeLua("box.space.basic_test:insert{1, '1'}");
        testHelper.executeLua("box.space.basic_test:insert{10, '10'}");

        TarantoolClientOps<Integer, List<?>, Object, List<?>> clientOps = client.syncOps();
        clientOps.update("basic_test", Collections.singletonList(1), Arrays.asList("=", 1, "one"));
        clientOps.update("basic_test", Collections.singletonList(2), Arrays.asList("=", 1, "two"));
        clientOps.update("basic_test", Collections.singletonList(10), Arrays.asList("=", 1, "ten"));

        checkRawTupleResult(consoleSelect(1), Arrays.asList(1, "one"));
        assertEquals(Collections.emptyList(), consoleSelect(2));
        checkRawTupleResult(consoleSelect(10), Arrays.asList(10, "ten"));
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

        checkRawTupleResult(consoleSelect(1), Arrays.asList(1, "one"));
        checkRawTupleResult(consoleSelect(2), Arrays.asList(2, "002"));
        checkRawTupleResult(consoleSelect(10), Arrays.asList(10, "ten"));
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

    private List<?> consoleSelect(Object key) {
        return testHelper.evaluate(TestUtils.toLuaSelect("basic_test", key));
    }

}
