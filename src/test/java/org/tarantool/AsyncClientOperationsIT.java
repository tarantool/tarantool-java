package org.tarantool;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for asynchronous operations provided by {@link TarantoolClientImpl} class.
 */
public class AsyncClientOperationsIT extends AbstractTarantoolConnectorIT {
    private TarantoolClient client;

    @BeforeEach
    public void setup() {
        client = makeClient();
    }

    @AfterEach
    public void tearDown() {
        client.close();
    }

    @Test
    public void testPing() {
        // This ping is still synchronous due to API declaration returning void.
        client.asyncOps().ping();
    }

    @Test
    public void testClose() {
        assertTrue(client.isAlive());
        client.asyncOps().close();
        assertFalse(client.isAlive());
    }

    @Test
    public void testAsyncError() {
        // Attempt to insert duplicate key.
        final Future<List<?>> res = client.asyncOps().insert(SPACE_ID, Arrays.asList(1, "one"));

        // Check that error is delivered when trying to obtain future result.
        ExecutionException e = assertThrows(ExecutionException.class, new Executable() {
            @Override
            public void execute() throws ExecutionException, InterruptedException, TimeoutException {
                res.get(TIMEOUT, TimeUnit.MILLISECONDS);
            }
        });
        assertNotNull(e.getCause());
        assertTrue(TarantoolException.class.isAssignableFrom(e.getCause().getClass()));
    }

    @Test
    public void testOperations() throws ExecutionException, InterruptedException, TimeoutException {
        TarantoolClientOps<Integer, List<?>, Object, Future<List<?>>> ops = client.asyncOps();

        List<Future<List<?>>> futs = new ArrayList<Future<List<?>>>();

        futs.add(ops.insert(SPACE_ID, Arrays.asList(10, "10")));
        futs.add(ops.delete(SPACE_ID, Collections.singletonList(10)));

        futs.add(ops.insert(SPACE_ID, Arrays.asList(10, "10")));
        futs.add(ops.update(SPACE_ID, Collections.singletonList(10), Arrays.asList("=", 1, "ten")));

        futs.add(ops.replace(SPACE_ID, Arrays.asList(20, "20")));
        futs.add(ops.upsert(SPACE_ID, Collections.singletonList(20), Arrays.asList(20, "twenty"),
            Arrays.asList("=", 1, "twenty")));

        futs.add(ops.insert(SPACE_ID, Arrays.asList(30, "30")));
        futs.add(ops.call("box.space.basic_test:delete", Collections.singletonList(30)));

        // Wait completion of all operations.
        for (Future<List<?>> f : futs)
            f.get(TIMEOUT, TimeUnit.MILLISECONDS);

        // Check the effects.
        checkTupleResult(consoleSelect(SPACE_NAME, 10), Arrays.asList(10, "ten"));
        checkTupleResult(consoleSelect(SPACE_NAME, 20), Arrays.asList(20, "twenty"));
        assertEquals(consoleSelect(SPACE_NAME, 30), Collections.emptyList());
    }

    @Test
    public void testSelect() throws ExecutionException, InterruptedException, TimeoutException {
        Future<List<?>> fut = client.asyncOps().select(SPACE_ID, PK_INDEX_ID, Collections.singletonList(1), 0, 1,
            Iterator.EQ);

        List<?> res = fut.get(TIMEOUT, TimeUnit.MILLISECONDS);

        checkTupleResult(res, Arrays.asList(1, "one"));
    }

    @Test
    public void testEval() throws ExecutionException, InterruptedException, TimeoutException {
        Future<List<?>> fut = client.asyncOps().eval("return true");
        assertEquals(Collections.singletonList(true), fut.get(TIMEOUT, TimeUnit.MILLISECONDS));
    }

    @Test
    public void testCall() throws ExecutionException, InterruptedException, TimeoutException {
        Future<List<?>> fut = client.asyncOps().call("echo", "hello");
        assertEquals(Collections.singletonList(Collections.singletonList("hello")),
            fut.get(TIMEOUT, TimeUnit.MILLISECONDS));
    }
}
