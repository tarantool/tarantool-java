package org.tarantool.schema;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.tarantool.TestUtils.makeDefaultClientConfig;
import static org.tarantool.TestUtils.makeTestClient;

import org.tarantool.TarantoolClient;
import org.tarantool.TarantoolClientConfig;
import org.tarantool.TarantoolClientOps;
import org.tarantool.TarantoolTestHelper;
import org.tarantool.TarantoolThreadDaemonFactory;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

@DisplayName("A client")
public class ClientThreadSafeSchemaIT {

    private static TarantoolTestHelper testHelper;

    @BeforeAll
    public static void setupEnv() {
        testHelper = new TarantoolTestHelper("client-schema-thread-safe-it");
        testHelper.createInstance();
        testHelper.startInstance();
    }

    @AfterAll
    public static void teardownEnv() {
        testHelper.stopInstance();
    }

    @Test
    @DisplayName("executed many DML/DDL string-operations from several threads simultaneously")
    void testFetchSpaces() {
        testHelper.executeLua(
            makeCreateSpaceFunction(),
            makeDropSpaceFunction()
        );

        TarantoolClientConfig config = makeDefaultClientConfig();
        config.operationExpiryTimeMillis = 2000;
        TarantoolClient client = makeTestClient(config, 500);

        int threadsNumber = 16;
        int iterations = 100;
        final CountDownLatch latch = new CountDownLatch(threadsNumber);
        ExecutorService executor = Executors.newFixedThreadPool(
            threadsNumber,
            new TarantoolThreadDaemonFactory("testWorkers")
        );

        // multiple threads can cause schema invalidation simultaneously
        // but it hasn't to affect other threads
        for (int i = 0; i < threadsNumber; i++) {
            int threadNumber = i;
            executor.submit(() -> {
                String spaceName = "my_space" + threadNumber;
                for (int k = 0; k < iterations; k++) {
                    TarantoolClientOps<Integer, List<?>, Object, List<?>> ops = client.syncOps();
                    ops.call("makeSpace", spaceName);
                    ops.insert(spaceName, Arrays.asList(k, threadNumber));
                    ops.call("dropSpace", spaceName);
                }
                latch.countDown();
            });
        }

        try {
            assertTrue(latch.await(20, TimeUnit.SECONDS));
        } catch (InterruptedException e) {
            fail(e);
        } finally {
            executor.shutdownNow();
            client.close();
        }
    }

    private String makeCreateSpaceFunction() {
        return "function makeSpace(spaceName) " +
            "box.schema.space.create(spaceName, { format = " +
            "{ {name = 'id', type = 'integer'}, " +
            "  {name = 'counts', type = 'integer'} } " +
            "}); " +
            "box.space[spaceName]:create_index('pk', { type = 'TREE', parts = {'id'} } ) " +
            "end";
    }

    private String makeDropSpaceFunction() {
        return "function dropSpace(spaceName) " +
            "box.space[spaceName]:drop() " +
            "end";
    }

}
