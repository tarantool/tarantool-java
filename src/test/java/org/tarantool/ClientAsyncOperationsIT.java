package org.tarantool;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.tarantool.TestAssertions.checkRawTupleResult;

import org.tarantool.dsl.TarantoolRequestSpec;
import org.tarantool.schema.TarantoolIndexNotFoundException;
import org.tarantool.schema.TarantoolSpaceNotFoundException;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Stream;

/**
 * Class with test cases for asynchronous operations
 * <p>
 * NOTE: Parametrized tests can be simplified after
 * https://github.com/junit-team/junit5/issues/878
 */
class ClientAsyncOperationsIT {

    private static final int TIMEOUT = 500;

    private static final String[] SETUP_SCRIPT = new String[] {
        "box.schema.space.create('basic_test', { format = " +
            "{{name = 'id', type = 'integer'}," +
            " {name = 'val', type = 'string'} } })",

        "box.space.basic_test:create_index('pk', { type = 'TREE', parts = {'id'} } )",
    };

    private static final String[] CLEAN_SCRIPT = new String[] {
        "box.space.basic_test and box.space.basic_test:drop()",
    };

    private static TarantoolTestHelper testHelper;

    private int spaceId;
    private int pkIndexId;

    static Stream<AsyncOpsProvider> getAsyncOps() {
        return Stream.of(new ClientAsyncOpsProvider(), new ComposableAsyncOpsProvider());
    }

    @BeforeAll
    static void setupEnv() {
        testHelper = new TarantoolTestHelper("async-ops-it");
        testHelper.createInstance();
        testHelper.startInstance();
    }

    @AfterAll
    static void cleanupEnv() {
        testHelper.stopInstance();
    }

    @BeforeEach
    void setup() {
        testHelper.executeLua(SETUP_SCRIPT);
        spaceId = testHelper.evaluate("box.space.basic_test.id");
        pkIndexId = testHelper.evaluate("box.space.basic_test.index.pk.id");
    }

    @AfterEach
    void tearDown() {
        testHelper.executeLua(CLEAN_SCRIPT);
    }

    @ParameterizedTest
    @MethodSource("getAsyncOps")
    void testPing(AsyncOpsProvider provider) {
        // This ping is still synchronous due to API declaration returning void.
        provider.getAsyncOps().ping();

        provider.close();
    }

    @ParameterizedTest
    @MethodSource("getAsyncOps")
    void testClose(AsyncOpsProvider provider) {
        TarantoolClient client = provider.getClient();
        assertTrue(client.isAlive());
        provider.getAsyncOps().close();
        assertFalse(client.isAlive());

        provider.close();
    }

    @ParameterizedTest
    @MethodSource("getAsyncOps")
    void testAsyncError(AsyncOpsProvider provider) {
        testHelper.executeLua("box.space.basic_test:replace{1, 'one'}");

        // Attempt to insert duplicate key.
        final Future<List<?>> res = provider.getAsyncOps().insert(spaceId, Arrays.asList(1, "one"));

        // Check that error is delivered when trying to obtain future result.
        ExecutionException e = assertThrows(ExecutionException.class, () -> res.get(TIMEOUT, TimeUnit.MILLISECONDS));
        assertNotNull(e.getCause());
        assertTrue(TarantoolException.class.isAssignableFrom(e.getCause().getClass()));

        provider.close();
    }

    @ParameterizedTest
    @MethodSource("getAsyncOps")
    void testOperations(AsyncOpsProvider provider)
        throws ExecutionException, InterruptedException, TimeoutException {
        testHelper.executeLua(
            "box.space.basic_test:insert{10, '10'}",
            "box.space.basic_test:insert{20, '20'}",
            "box.space.basic_test:insert{30, '30'}",
            "box.space.basic_test:insert{40, '40'}",
            "box.space.basic_test:insert{50, '50'}"
        );

        TarantoolClientOps<Integer, List<?>, Object, Future<List<?>>> ops = provider.getAsyncOps();
        List<Future<List<?>>> futures = new ArrayList<>();

        futures.add(ops.delete(spaceId, Collections.singletonList(10)));

        futures.add(ops.insert(spaceId, Arrays.asList(60, "60")));

        futures.add(ops.update(spaceId, Collections.singletonList(50), Arrays.asList("=", 1, "fifty")));

        futures.add(ops.replace(spaceId, Arrays.asList(30, "thirty")));
        futures.add(ops.replace(spaceId, Arrays.asList(70, "70")));

        futures.add(
            ops.upsert(
                spaceId,
                Collections.singletonList(20),
                Arrays.asList(20, "20"),
                Arrays.asList("=", 1, "twenty")
            )
        );
        futures.add(
            ops.upsert(
                spaceId,
                Collections.singletonList(80),
                Arrays.asList(80, "80"),
                Arrays.asList("=", 1, "eighty")
            )
        );
        futures.add(ops.call("box.space.basic_test:delete", Collections.singletonList(40)));

        // Wait completion of all operations.
        for (Future<List<?>> f : futures) {
            f.get(TIMEOUT, TimeUnit.MILLISECONDS);
        }

        // Check the effects.
        assertEquals(Collections.emptyList(), consoleSelect(10));
        checkRawTupleResult(consoleSelect(20), Arrays.asList(20, "twenty"));
        checkRawTupleResult(consoleSelect(30), Arrays.asList(30, "thirty"));
        assertEquals(Collections.emptyList(), consoleSelect(40));
        checkRawTupleResult(consoleSelect(50), Arrays.asList(50, "fifty"));
        checkRawTupleResult(consoleSelect(60), Arrays.asList(60, "60"));
        checkRawTupleResult(consoleSelect(70), Arrays.asList(70, "70"));
        checkRawTupleResult(consoleSelect(80), Arrays.asList(80, "80"));
        provider.close();
    }

    @ParameterizedTest
    @MethodSource("getAsyncOps")
    void testSelect(AsyncOpsProvider provider) throws ExecutionException, InterruptedException, TimeoutException {
        testHelper.executeLua("box.space.basic_test:replace{1, 'one'}");

        Future<List<?>> fut = provider.getAsyncOps()
            .select(spaceId, pkIndexId, Collections.singletonList(1), 0, 1, Iterator.EQ);

        List<?> res = fut.get(TIMEOUT, TimeUnit.MILLISECONDS);

        checkRawTupleResult(res, Arrays.asList(1, "one"));

        provider.close();
    }

    @ParameterizedTest
    @MethodSource("getAsyncOps")
    void testEval(AsyncOpsProvider provider) throws ExecutionException, InterruptedException, TimeoutException {
        Future<List<?>> fut = provider.getAsyncOps().eval("return true");
        assertEquals(Collections.singletonList(true), fut.get(TIMEOUT, TimeUnit.MILLISECONDS));

        provider.close();
    }

    @ParameterizedTest
    @MethodSource("getAsyncOps")
    void testCall(AsyncOpsProvider provider) throws ExecutionException, InterruptedException, TimeoutException {
        testHelper.executeLua("function echo(...) return ... end");

        Future<List<?>> fut = provider.getAsyncOps().call("echo", "hello");
        assertEquals(Collections.singletonList("hello"), fut.get(TIMEOUT, TimeUnit.MILLISECONDS));

        provider.close();
    }

    @ParameterizedTest
    @MethodSource("getAsyncOps")
    void testStringSelect(AsyncOpsProvider provider) throws ExecutionException, InterruptedException, TimeoutException {
        testHelper.executeLua("box.space.basic_test:insert{1, 'one'}");
        Future<List<?>> result = provider.getAsyncOps()
            .select("basic_test", "pk", Collections.singletonList(1), 0, 1, Iterator.EQ);

        assertEquals(
            Collections.singletonList(Arrays.asList(1, "one")),
            result.get(TIMEOUT, TimeUnit.MILLISECONDS)
        );

        provider.close();
    }

    @ParameterizedTest
    @MethodSource("getAsyncOps")
    void testStringInsert(AsyncOpsProvider provider) throws ExecutionException, InterruptedException, TimeoutException {
        Future<List<?>> resultOne = provider.getAsyncOps()
            .insert("basic_test", Arrays.asList(1, "one"));

        Future<List<?>> resultTen = provider.getAsyncOps()
            .insert("basic_test", Arrays.asList(10, "ten"));

        resultOne.get(TIMEOUT, TimeUnit.MILLISECONDS);
        resultTen.get(TIMEOUT, TimeUnit.MILLISECONDS);

        checkRawTupleResult(consoleSelect(1), Arrays.asList(1, "one"));
        checkRawTupleResult(consoleSelect(10), Arrays.asList(10, "ten"));

        provider.close();
    }

    @ParameterizedTest
    @MethodSource("getAsyncOps")
    void testStringReplace(AsyncOpsProvider provider)
        throws ExecutionException, InterruptedException, TimeoutException {
        testHelper.executeLua("box.space.basic_test:insert{1, '1'}");
        testHelper.executeLua("box.space.basic_test:insert{10, '10'}");

        Future<List<?>> resultOne = provider.getAsyncOps()
            .replace("basic_test", Arrays.asList(1, "one"));

        Future<List<?>> resultTen = provider.getAsyncOps()
            .replace("basic_test", Arrays.asList(10, "ten"));

        resultOne.get(TIMEOUT, TimeUnit.MILLISECONDS);
        resultTen.get(TIMEOUT, TimeUnit.MILLISECONDS);

        checkRawTupleResult(consoleSelect(1), Arrays.asList(1, "one"));
        checkRawTupleResult(consoleSelect(10), Arrays.asList(10, "ten"));

        provider.close();
    }

    @ParameterizedTest
    @MethodSource("getAsyncOps")
    void testStringDelete(AsyncOpsProvider provider) throws ExecutionException, InterruptedException, TimeoutException {
        testHelper.executeLua("box.space.basic_test:insert{1, '1'}");
        testHelper.executeLua("box.space.basic_test:insert{10, '10'}");
        testHelper.executeLua("box.space.basic_test:insert{20, '20'}");

        Future<List<?>> resultOne = provider.getAsyncOps()
            .delete("basic_test", Collections.singletonList(1));

        Future<List<?>> resultTwenty = provider.getAsyncOps()
            .delete("basic_test", Collections.singletonList(20));

        resultOne.get(TIMEOUT, TimeUnit.MILLISECONDS);
        resultTwenty.get(TIMEOUT, TimeUnit.MILLISECONDS);

        assertEquals(Collections.emptyList(), consoleSelect(1));
        checkRawTupleResult(consoleSelect(10), Arrays.asList(10, "10"));
        assertEquals(Collections.emptyList(), consoleSelect(20));

        provider.close();
    }

    @ParameterizedTest
    @MethodSource("getAsyncOps")
    void testStringUpdate(AsyncOpsProvider provider) throws ExecutionException, InterruptedException, TimeoutException {
        testHelper.executeLua("box.space.basic_test:insert{1, '1'}");
        testHelper.executeLua("box.space.basic_test:insert{10, '10'}");

        Future<List<?>> resultOne = provider.getAsyncOps()
            .update("basic_test", Collections.singletonList(1), Arrays.asList("=", 1, "one"));

        Future<List<?>> resultTwo = provider.getAsyncOps()
            .update("basic_test", Collections.singletonList(2), Arrays.asList("=", 1, "two"));

        Future<List<?>> resultTen = provider.getAsyncOps()
            .update("basic_test", Collections.singletonList(10), Arrays.asList("=", 1, "ten"));

        resultOne.get(TIMEOUT, TimeUnit.MILLISECONDS);
        resultTwo.get(TIMEOUT, TimeUnit.MILLISECONDS);
        resultTen.get(TIMEOUT, TimeUnit.MILLISECONDS);

        checkRawTupleResult(consoleSelect(1), Arrays.asList(1, "one"));
        assertEquals(Collections.emptyList(), consoleSelect(2));
        checkRawTupleResult(consoleSelect(10), Arrays.asList(10, "ten"));

        provider.close();
    }

    @ParameterizedTest
    @MethodSource("getAsyncOps")
    void testStringUpsert(AsyncOpsProvider provider) throws ExecutionException, InterruptedException, TimeoutException {
        testHelper.executeLua("box.space.basic_test:insert{1, '1'}");
        testHelper.executeLua("box.space.basic_test:insert{10, '10'}");

        Future<List<?>> resultOne = provider.getAsyncOps()
            .upsert("basic_test", Collections.singletonList(1), Arrays.asList(1, "001"), Arrays.asList("=", 1, "one"));

        Future<List<?>> resultTwo = provider.getAsyncOps()
            .upsert("basic_test", Collections.singletonList(2), Arrays.asList(2, "002"), Arrays.asList("=", 1, "two"));

        Future<List<?>> resultTen = provider.getAsyncOps()
            .upsert("basic_test", Collections.singletonList(10), Arrays.asList(10, "010"),
                Arrays.asList("=", 1, "ten"));

        resultOne.get(TIMEOUT, TimeUnit.MILLISECONDS);
        resultTwo.get(TIMEOUT, TimeUnit.MILLISECONDS);
        resultTen.get(TIMEOUT, TimeUnit.MILLISECONDS);

        checkRawTupleResult(consoleSelect(1), Arrays.asList(1, "one"));
        checkRawTupleResult(consoleSelect(2), Arrays.asList(2, "002"));
        checkRawTupleResult(consoleSelect(10), Arrays.asList(10, "ten"));

        provider.close();
    }

    @ParameterizedTest
    @MethodSource("getAsyncOps")
    void testStringMultipleIndirectChanges(AsyncOpsProvider provider)
        throws ExecutionException, InterruptedException, TimeoutException {
        testHelper.executeLua("box.space.basic_test:insert{1, 'one'}");
        Future<List<?>> result = provider.getAsyncOps()
            .select("basic_test", "pk", Collections.singletonList(1), 0, 1, Iterator.EQ);

        assertEquals(
            Collections.singletonList(Arrays.asList(1, "one")),
            result.get(TIMEOUT, TimeUnit.MILLISECONDS)
        );

        testHelper.executeLua("box.space.basic_test and box.space.basic_test:drop()");
        testHelper.executeLua(
            "box.schema.space.create('basic_test', { format = " +
                "{{name = 'id', type = 'integer'}," +
                " {name = 'val', type = 'string'} } })",

            "box.space.basic_test:create_index('pk', { type = 'TREE', parts = {'id'} } )"
        );
        testHelper.executeLua("box.space.basic_test:insert{2, 'two'}");

        result = provider.getAsyncOps()
            .select("basic_test", "pk", Collections.singletonList(2), 0, 1, Iterator.EQ);

        assertEquals(
            Collections.singletonList(Arrays.asList(2, "two")),
            result.get(TIMEOUT, TimeUnit.MILLISECONDS)
        );

        testHelper.executeLua("box.space.basic_test and box.space.basic_test:drop()");
        testHelper.executeLua(
            "box.schema.space.create('basic_test', { format = " +
                "{{name = 'id', type = 'integer'}," +
                " {name = 'val', type = 'string'} } })",

            "box.space.basic_test:create_index('pk', { type = 'TREE', parts = {'id'} } )"
        );
        testHelper.executeLua("box.space.basic_test:insert{3, 'three'}");

        result = provider.getAsyncOps()
            .select("basic_test", "pk", Collections.singletonList(3), 0, 1, Iterator.EQ);

        assertEquals(
            Collections.singletonList(Arrays.asList(3, "three")),
            result.get(TIMEOUT, TimeUnit.MILLISECONDS)
        );

        provider.close();
    }

    @ParameterizedTest
    @MethodSource("getAsyncOps")
    void testUnknownSpace(AsyncOpsProvider provider) throws ExecutionException, InterruptedException, TimeoutException {
        Future<List<?>> resultOne = provider.getAsyncOps()
            .update("basic_test_unknown", Collections.singletonList(1), Arrays.asList("=", 1, "one"));

        Exception exception = assertThrows(Exception.class, () -> resultOne.get(TIMEOUT, TimeUnit.MILLISECONDS));
        assertTrue(exception.getCause() instanceof TarantoolSpaceNotFoundException);

        provider.close();
    }

    @ParameterizedTest
    @MethodSource("getAsyncOps")
    void testUnknownSpaceIndex(AsyncOpsProvider provider) {
        Future<List<?>> resultOne = provider.getAsyncOps()
            .select("basic_test", "pk_unknown", Collections.singletonList(3), 0, 1, Iterator.EQ);

        Exception exception = assertThrows(Exception.class, () -> resultOne.get(TIMEOUT, TimeUnit.MILLISECONDS));
        assertTrue(exception.getCause() instanceof TarantoolIndexNotFoundException);

        provider.close();
    }

    private List<?> consoleSelect(Object key) {
        return testHelper.evaluate(TestUtils.toLuaSelect("basic_test", key));
    }

    private interface AsyncOpsProvider {

        TarantoolClientOps<Integer, List<?>, Object, Future<List<?>>> getAsyncOps();

        TarantoolClient getClient();

        void close();

    }

    private static class ClientAsyncOpsProvider implements AsyncOpsProvider {

        TarantoolClient client = TestUtils.makeTestClient(TestUtils.makeDefaultClientConfig(), 2000);

        @Override
        public TarantoolClientOps<Integer, List<?>, Object, Future<List<?>>> getAsyncOps() {
            return client.asyncOps();
        }

        @Override
        public TarantoolClient getClient() {
            return client;
        }

        @Override
        public void close() {
            client.close();
        }

    }

    private static class ComposableAsyncOpsProvider implements AsyncOpsProvider {

        TarantoolClient client = TestUtils.makeTestClient(TestUtils.makeDefaultClientConfig(), 2000);
        TarantoolClientOps<Integer, List<?>, Object, Future<List<?>>> composableOps =
            new Composable2FutureClientOpsAdapter(client.composableAsyncOps());

        @Override
        public TarantoolClientOps<Integer, List<?>, Object, Future<List<?>>> getAsyncOps() {
            return composableOps;
        }

        @Override
        public TarantoolClient getClient() {
            return client;
        }

        @Override
        public void close() {
            client.close();
        }

    }

    private static class Composable2FutureClientOpsAdapter
        implements TarantoolClientOps<Integer, List<?>, Object, Future<List<?>>> {

        private final TarantoolClientOps<Integer, List<?>, Object, CompletionStage<List<?>>> originOps;

        private Composable2FutureClientOpsAdapter(
            TarantoolClientOps<Integer, List<?>, Object, CompletionStage<List<?>>> originOps) {

            this.originOps = originOps;
        }

        @Override
        public Future<List<?>> select(Integer space, Integer index, List<?> key, int offset, int limit, int iterator) {
            return originOps.select(space, index, key, offset, limit, iterator).toCompletableFuture();
        }

        @Override
        public Future<List<?>> select(String space, String index, List<?> key, int offset, int limit, int iterator) {
            return originOps.select(space, index, key, offset, limit, iterator).toCompletableFuture();
        }

        @Override
        public Future<List<?>> select(Integer space,
                                      Integer index,
                                      List<?> key,
                                      int offset,
                                      int limit,
                                      Iterator iterator) {
            return originOps.select(space, index, key, offset, limit, iterator).toCompletableFuture();
        }

        @Override
        public Future<List<?>> select(String space,
                                      String index,
                                      List<?> key,
                                      int offset,
                                      int limit,
                                      Iterator iterator) {
            return originOps.select(space, index, key, offset, limit, iterator).toCompletableFuture();
        }

        @Override
        public Future<List<?>> insert(Integer space, List<?> tuple) {
            return originOps.insert(space, tuple).toCompletableFuture();
        }

        @Override
        public Future<List<?>> insert(String space, List<?> tuple) {
            return originOps.insert(space, tuple).toCompletableFuture();
        }

        @Override
        public Future<List<?>> replace(Integer space, List<?> tuple) {
            return originOps.replace(space, tuple).toCompletableFuture();
        }

        @Override
        public Future<List<?>> replace(String space, List<?> tuple) {
            return originOps.replace(space, tuple).toCompletableFuture();
        }

        @Override
        public Future<List<?>> update(Integer space, List<?> key, Object... tuple) {
            return originOps.update(space, key, tuple).toCompletableFuture();
        }

        @Override
        public Future<List<?>> update(String space, List<?> key, Object... tuple) {
            return originOps.update(space, key, tuple).toCompletableFuture();
        }

        @Override
        public Future<List<?>> upsert(Integer space, List<?> key, List<?> defTuple, Object... ops) {
            return originOps.upsert(space, key, defTuple, ops).toCompletableFuture();
        }

        @Override
        public Future<List<?>> upsert(String space, List<?> key, List<?> defTuple, Object... ops) {
            return originOps.upsert(space, key, defTuple, ops).toCompletableFuture();
        }

        @Override
        public Future<List<?>> delete(Integer space, List<?> key) {
            return originOps.delete(space, key).toCompletableFuture();
        }

        @Override
        public Future<List<?>> delete(String space, List<?> key) {
            return originOps.delete(space, key).toCompletableFuture();
        }

        @Override
        public Future<List<?>> call(String function, Object... args) {
            return originOps.call(function, args).toCompletableFuture();
        }

        @Override
        public Future<List<?>> eval(String expression, Object... args) {
            return originOps.eval(expression, args).toCompletableFuture();
        }

        @Override
        public Future<List<?>> execute(TarantoolRequestSpec requestSpec) {
            return originOps.execute(requestSpec).toCompletableFuture();
        }

        @Override
        public void ping() {
            originOps.ping();
        }

        @Override
        public void close() {
            originOps.close();
        }

    }

}
