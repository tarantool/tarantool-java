package org.tarantool;

import java.util.List;
import java.util.concurrent.Future;

/**
 * Tests for asynchronous operations provided by {@link TarantoolClientImpl} class.
 */
public class AsyncClientOperationsIT extends AbstractAsyncClientOperationsIT {
    @Override
    protected TarantoolClientOps<Integer, List<?>, Object, Future<List<?>>> getOps() {
        return getClient().asyncOps();
    }
}
