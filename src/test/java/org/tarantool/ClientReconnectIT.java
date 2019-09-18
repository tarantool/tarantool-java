package org.tarantool;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.tarantool.TestUtils.findCause;
import static org.tarantool.TestUtils.makeDefaultClientConfig;
import static org.tarantool.TestUtils.makeTestClient;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.net.ConnectException;
import java.nio.channels.SocketChannel;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.AtomicReferenceArray;
import java.util.concurrent.locks.LockSupport;

public class ClientReconnectIT {

    private static final int RESTART_TIMEOUT = 2000;
    private static final int TIMEOUT = 500;

    private static final SocketChannelProvider socketChannelProvider =
        new TestSocketChannelProvider(TarantoolTestHelper.HOST, TarantoolTestHelper.PORT, RESTART_TIMEOUT);
    private static TarantoolTestHelper testHelper;

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
    private int pkIndexId;

    @BeforeAll
    static void setupEnv() {
        testHelper = new TarantoolTestHelper("client-reconnect-it");
        testHelper.createInstance();
    }

    @BeforeEach
    void setUpTest() {
        testHelper.startInstance();
        testHelper.executeLua(SETUP_SCRIPT);

        spaceId = testHelper.evaluate("box.space.basic_test.id");
        pkIndexId = testHelper.evaluate("box.space.basic_test.index.pk.id");
    }

    @AfterEach
    public void tearDown() {
        if (client != null) {
            assertTimeoutPreemptively(
                Duration.ofMillis(RESTART_TIMEOUT),
                () -> client.close(),
                "Close is stuck."
            );
        }
        testHelper.executeLua(CLEAN_SCRIPT);
        testHelper.stopInstance();
    }

    @Test
    public void testReconnect() throws Exception {
        client = makeTestClient(makeDefaultClientConfig(), RESTART_TIMEOUT);

        client.syncOps().ping();

        testHelper.stopInstance();

        Exception e = assertThrows(Exception.class, () -> client.syncOps().ping());

        assertTrue(CommunicationException.class.isAssignableFrom(e.getClass()) ||
            IllegalStateException.class.isAssignableFrom(e.getClass()));

        assertNotNull(((TarantoolClientImpl) client).getThumbstone());

        assertFalse(client.isAlive());

        testHelper.startInstance();

        assertTrue(client.waitAlive(TIMEOUT, TimeUnit.MILLISECONDS));

        client.syncOps().ping();
    }

    /**
     * Spurious return from LockSupport.park() must not lead to reconnect.
     * The implementation must check some invariant to tell a spurious
     * return from the intended one.
     */
    @Test
    public void testSpuriousReturnFromPark() {
        final CountDownLatch latch = new CountDownLatch(2);
        SocketChannelProvider provider = (retryNumber, lastError) -> {
            if (lastError == null) {
                latch.countDown();
            }
            return socketChannelProvider.get(retryNumber, lastError);
        };

        client = new TarantoolClientImpl(provider, makeDefaultClientConfig());
        client.syncOps().ping();

        // The park() will return inside connector thread.
        LockSupport.unpark(((TarantoolClientImpl) client).connector);

        // Wait on latch as a proof that reconnect did not happen.
        // In case of a failure, latch will reach 0 before timeout occurs.
        try {
            assertFalse(latch.await(TIMEOUT, TimeUnit.MILLISECONDS));
        } catch (InterruptedException e) {
            fail();
        }
    }

    /**
     * When the client is closed, all outstanding operations must fail.
     * Otherwise, synchronous wait on such operations will block forever.
     */
    @Test
    public void testCloseWhileOperationsAreInProgress() {
        client = new TarantoolClientImpl(socketChannelProvider, makeDefaultClientConfig()) {
            @Override
            protected void write(Code code, Long syncId, Long schemaId, Object... args) {
                // Skip write.
            }
        };

        final Future<List<?>> res = client.asyncOps()
            .select(spaceId, pkIndexId, Collections.singletonList(1), 0, 1, Iterator.EQ);

        client.close();

        ExecutionException e = assertThrows(ExecutionException.class, res::get);
    }

    /**
     * When the reconnection happen, the outstanding operations must fail.
     * Otherwise, synchronous wait on such operations will block forever.
     */
    @Test
    public void testReconnectWhileOperationsAreInProgress() {
        final AtomicBoolean writeEnabled = new AtomicBoolean(false);
        client = new TarantoolClientImpl(socketChannelProvider, makeDefaultClientConfig()) {
            @Override
            protected void write(Code code, Long syncId, Long schemaId, Object... args) throws Exception {
                if (writeEnabled.get()) {
                    super.write(code, syncId, schemaId, args);
                }
            }
        };

        final Future<List<?>> mustFail = client.asyncOps()
            .select(spaceId, pkIndexId, Collections.singletonList(1), 0, 1, Iterator.EQ);

        testHelper.stopInstance();

        ExecutionException executionException = assertThrows(ExecutionException.class, mustFail::get);
        assertEquals(executionException.getCause().getClass(), CommunicationException.class);

        writeEnabled.set(true);
        testHelper.startInstance();


        try {
            client.waitAlive(RESTART_TIMEOUT, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            fail();
        }

        Future<List<?>> res = client.asyncOps()
            .select(spaceId, pkIndexId, Collections.singletonList(1), 0, 1, Iterator.EQ);

        try {
            res.get(TIMEOUT, TimeUnit.MILLISECONDS);
        } catch (Exception e) {
            fail(e);
        }
    }

    @Test
    public void testConcurrentCloseAndReconnect() {
        final CountDownLatch latch = new CountDownLatch(2);
        client = new TarantoolClientImpl(socketChannelProvider, makeDefaultClientConfig()) {
            @Override
            protected void connect(final SocketChannel channel) throws Exception {
                latch.countDown();
                super.connect(channel);
            }
        };

        testHelper.stopInstance();
        testHelper.startInstance();

        try {
            assertTrue(latch.await(RESTART_TIMEOUT, TimeUnit.MILLISECONDS));
        } catch (InterruptedException e) {
            fail(e);
        }
    }

    // DO NOT REMOVE THIS TEST
    // Motivation: this test checks start/stop correctness
    // of TarantoolControl class which is used by other tests.
    // This test is commented out because the class is used
    // for internal purposes only and isn't related to
    // the connector testing.
    //    @Test
    //    @DisplayName("follow up the issue #164")
    //    void testStartStopTarantoolInstance() throws InterruptedException {
    //        int numberOfParallelInstances = 4;
    //        CountDownLatch finished = new CountDownLatch(numberOfParallelInstances);
    //        List<String> instancesNames = new ArrayList<>(numberOfParallelInstances);
    //
    //        for (int i = 0; i < numberOfParallelInstances; i++) {
    //            String instance = "startStop" + (i + 1);
    //            instancesNames.add(instance);
    //            control.createInstance(
    //                instancesNames.get(i),
    //                LUA_FILE,
    //                makeInstanceEnv(3401 + i + 1, 3501 + i + 1)
    //            );
    //            startTarantool(instancesNames.get(i));
    //            new Thread(() -> {
    //                for (int j = 0; j < 100; j++) {
    //                    stopTarantool(instance);
    //                    startTarantool(instance);
    //                    if (j % 10 == 0) {
    //                        System.out.println(
    //                            Thread.currentThread().getName() + ": " + j + "% completed"
    //                        );
    //                    }
    //                }
    //                finished.countDown();
    //            }, "Thread" + (i + 1)).start();
    //        }
    //
    //        assertTrue(finished.await(2, TimeUnit.MINUTES));
    //
    //        for (int i = 0; i < numberOfParallelInstances; i++) {
    //            stopTarantool(instancesNames.get(i));
    //        }
    //    }

    /**
     * Test concurrent operations, reconnects and close.
     * <p>
     * Expected situation is nothing gets stuck.
     * <p>
     * The test sets SO_LINGER to 0 for outgoing connections to avoid producing
     * many TIME_WAIT sockets, because an available port range can be
     * exhausted.
     */
    @Test
    public void testLongParallelCloseReconnects() {
        int numThreads = 4;
        int numClients = 4;
        int timeBudget = 30 * 1000;

        SocketChannelProvider provider = makeZeroLingerProvider();
        final AtomicReferenceArray<TarantoolClient> clients =
            new AtomicReferenceArray<>(numClients);

        for (int idx = 0; idx < clients.length(); idx++) {
            clients.set(idx, makeClient(provider));
        }

        final Random rnd = new Random();
        final AtomicInteger cnt = new AtomicInteger();

        // Start background threads that do operations.
        final CountDownLatch latch = new CountDownLatch(numThreads);
        final long deadline = System.currentTimeMillis() + timeBudget;
        Thread[] threads = new Thread[numThreads];
        for (int idx = 0; idx < threads.length; idx++) {
            threads[idx] = new Thread(new Runnable() {
                @Override
                public void run() {
                    while (!Thread.currentThread().isInterrupted() && deadline > System.currentTimeMillis()) {
                        int idx = rnd.nextInt(clients.length());
                        try {
                            TarantoolClient cli = clients.get(idx);

                            int maxOps = rnd.nextInt(100);
                            for (int n = 0; n < maxOps; n++) {
                                cli.syncOps().ping();
                            }

                            cli.close();

                            TarantoolClient next = makeClient(provider);
                            if (!clients.compareAndSet(idx, cli, next)) {
                                next.close();
                            }
                            cnt.incrementAndGet();
                        } catch (Exception ignored) {
                            // No-op.
                        }
                    }
                    latch.countDown();
                }
            });
        }

        for (int idx = 0; idx < threads.length; idx++) {
            threads[idx].start();
        }

        // Restart tarantool several times in the foreground.
        while (deadline > System.currentTimeMillis()) {
            testHelper.stopInstance();
            testHelper.startInstance();
            try {
                Thread.sleep(RESTART_TIMEOUT * 2);
            } catch (InterruptedException e) {
                fail(e);
            }
            if (deadline > System.currentTimeMillis()) {
                System.out.println(
                    "testLongParallelCloseReconnects: " +
                        (deadline - System.currentTimeMillis()) / 1000 +
                        "s remain"
                );
            }
        }

        // Wait for all threads to finish.
        try {
            assertTrue(latch.await(RESTART_TIMEOUT * 2, TimeUnit.MILLISECONDS));
        } catch (InterruptedException e) {
            fail(e);
        }

        // Close outstanding clients.
        for (int idx = 0; idx < clients.length(); idx++) {
            clients.get(idx).close();
        }

        assertTrue(cnt.get() > threads.length);
    }

    /**
     * Verify that we don't exceed a file descriptor limit (and so likely don't
     * leak file descriptors) when trying to connect to an existing node with
     * wrong authentication credentials.
     * <p>
     * The test sets SO_LINGER to 0 for outgoing connections to avoid producing
     * many TIME_WAIT sockets, because an available port range can be
     * exhausted.
     */
    @Test
    public void testReconnectWrongAuth() throws Exception {
        SocketChannelProvider provider = makeZeroLingerProvider();
        TarantoolClientConfig config = makeDefaultClientConfig();
        config.initTimeoutMillis = 100;
        config.password = config.password + 'x';
        for (int i = 0; i < 100; ++i) {
            if (i % 10 == 0) {
                System.out.println("testReconnectWrongAuth: " + (100 - i) + " iterations remain");
            }
            CommunicationException e = assertThrows(
                CommunicationException.class,
                () -> client = new TarantoolClientImpl(provider, config)
            );
            assertEquals(e.getMessage(), "100ms is exceeded when waiting " +
                "for client initialization. You could configure init " +
                "timeout in TarantoolConfig"
            );
        }

        /*
         * Verify we don't exceed a file descriptor limit. If we exceed it, a
         * client will not able to connect to tarantool.
         */
        TarantoolClient client = makeTestClient(makeDefaultClientConfig(), RESTART_TIMEOUT);
        client.syncOps().ping();
        client.close();
    }

    @Test
    void testFirstConnectionRefused() {
        RuntimeException error = new RuntimeException("Fake error");
        TarantoolClientConfig config = makeDefaultClientConfig();
        config.initTimeoutMillis = 100;
        Throwable exception = assertThrows(
            CommunicationException.class,
            () -> new TarantoolClientImpl(makeErroredProvider(error), config)
        );
        assertTrue(findCause(exception, error));
    }

    @Test
    void testConnectionRefusedAfterConnect() {
        TarantoolClientImpl client = new TarantoolClientImpl(makeErroredProvider(null), makeDefaultClientConfig());
        client.ping();

        testHelper.stopInstance();
        CommunicationException exception = assertThrows(CommunicationException.class, client::ping);

        Throwable origin = exception.getCause();
        assertEquals(origin, client.getThumbstone());

        testHelper.startInstance();
    }

    @Test
    void testSocketProviderRefusedByFakeReason() {
        TarantoolClientConfig config = makeDefaultClientConfig();
        RuntimeException error = new RuntimeException("Fake error");
        config.initTimeoutMillis = 1000;

        SingleSocketChannelProviderImpl socketProvider = new SingleSocketChannelProviderImpl("localhost:3301");

        testHelper.stopInstance();
        Throwable exception = assertThrows(
            CommunicationException.class,
            () -> new TarantoolClientImpl(TestUtils.wrapByErroredProvider(socketProvider, error), config)
        );
        testHelper.startInstance();
        assertTrue(findCause(exception, error));
    }

    @Test
    void testSingleSocketProviderRefused() {
        testHelper.stopInstance();

        TarantoolClientConfig config = makeDefaultClientConfig();
        config.initTimeoutMillis = 1000;

        SingleSocketChannelProviderImpl socketProvider = new SingleSocketChannelProviderImpl("localhost:3301");

        Throwable exception = assertThrows(
            CommunicationException.class,
            () -> new TarantoolClientImpl(socketProvider, config)
        );
        testHelper.startInstance();
        assertTrue(findCause(exception, ConnectException.class));
    }

    @Test
    void testSingleSocketProviderRefusedAfterConnect() {
        TarantoolClientImpl client = new TarantoolClientImpl(socketChannelProvider, makeDefaultClientConfig());

        client.ping();
        testHelper.stopInstance();

        CommunicationException exception = assertThrows(CommunicationException.class, client::ping);
        Throwable origin = exception.getCause();
        assertEquals(origin, client.getThumbstone());

        testHelper.startInstance();
    }

    private SocketChannelProvider makeZeroLingerProvider() {
        return new TestSocketChannelProvider(
            TarantoolTestHelper.HOST, TarantoolTestHelper.PORT, RESTART_TIMEOUT
        ).setSoLinger(0);
    }

    private SocketChannelProvider makeErroredProvider(RuntimeException error) {
        return new SocketChannelProvider() {
            private final SocketChannelProvider delegate = makeZeroLingerProvider();
            private AtomicReference<RuntimeException> errorReference = new AtomicReference<>(error);

            @Override
            public SocketChannel get(int retryNumber, Throwable lastError) {
                RuntimeException rawError = errorReference.get();
                if (rawError != null) {
                    throw rawError;
                }
                return delegate.get(retryNumber, lastError);
            }
        };
    }

    private TarantoolClient makeClient(SocketChannelProvider provider) {
        return new TarantoolClientImpl(provider, makeDefaultClientConfig());
    }

}
