package org.tarantool;

import java.util.List;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Future;

/**
 * Tests for composable asynchronous operations provided by {@link TarantoolClientImpl} class.
 */
public class ClientComposableAsyncOpsIT extends AbstractAsyncClientOperationsIT {

    private static class Composable2FutureClientOpsAdapter implements TarantoolClientOps<Integer, List<?>, Object, Future<List<?>>> {

        private final TarantoolClientOps<Integer, List<?>, Object, CompletionStage<List<?>>> originOps;

        private Composable2FutureClientOpsAdapter(TarantoolClientOps<Integer, List<?>, Object, CompletionStage<List<?>>> originOps) {
            this.originOps = originOps;
        }

        @Override
        public Future<List<?>> select(Integer space, Integer index, List<?> key, int offset, int limit, int iterator) {
            return originOps.select(space, index, key, offset, limit, iterator).toCompletableFuture();
        }

        @Override
        public Future<List<?>> select(Integer space, Integer index, List<?> key, int offset, int limit, Iterator iterator) {
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

    @Override
    protected TarantoolClientOps<Integer, List<?>, Object, Future<List<?>>> getOps() {
        return new Composable2FutureClientOpsAdapter(getClient().composableAsyncOps());
    }
}
