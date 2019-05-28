package org.tarantool;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.tarantool.TestAssertions.checkRawTupleResult;

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
 *
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
        TarantoolClientOps<Integer, List<?>, Object, Future<List<?>>> ops = provider.getAsyncOps();

        List<Future<List<?>>> futures = new ArrayList<>();

        futures.add(ops.insert(spaceId, Arrays.asList(10, "10")));
        futures.add(ops.delete(spaceId, Collections.singletonList(10)));

        futures.add(ops.insert(spaceId, Arrays.asList(10, "10")));
        futures.add(ops.update(spaceId, Collections.singletonList(10), Arrays.asList("=", 1, "ten")));

        futures.add(ops.replace(spaceId, Arrays.asList(20, "20")));
        futures.add(ops.upsert(spaceId, Collections.singletonList(20), Arrays.asList(20, "twenty"),
            Arrays.asList("=", 1, "twenty")));

        futures.add(ops.insert(spaceId, Arrays.asList(30, "30")));
        futures.add(ops.call("box.space.basic_test:delete", Collections.singletonList(30)));

        // Wait completion of all operations.
        for (Future<List<?>> f : futures) {
            f.get(TIMEOUT, TimeUnit.MILLISECONDS);
        }

        // Check the effects.
        checkRawTupleResult(consoleSelect(10), Arrays.asList(10, "ten"));
        checkRawTupleResult(consoleSelect(20), Arrays.asList(20, "twenty"));
        assertEquals(consoleSelect(30), Collections.emptyList());

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
        assertEquals(Collections.singletonList(Collections.singletonList("hello")),
            fut.get(TIMEOUT, TimeUnit.MILLISECONDS));

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
            composableOps.close();
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
        public Future<List<?>> select(Integer space,
                                      Integer index,
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
        public Future<List<?>> replace(Integer space, List<?> tuple) {
            return originOps.replace(space, tuple).toCompletableFuture();
        }

        @Override
        public Future<List<?>> update(Integer space, List<?> key, Object... tuple) {
            return originOps.update(space, key, tuple).toCompletableFuture();
        }

        @Override
        public Future<List<?>> upsert(Integer space, List<?> key, List<?> defTuple, Object... ops) {
            return originOps.upsert(space, key, defTuple, ops).toCompletableFuture();
        }

        @Override
        public Future<List<?>> delete(Integer space, List<?> key) {
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
        public void ping() {
            originOps.ping();
        }

        @Override
        public void close() {
            originOps.close();
        }
    }

}
