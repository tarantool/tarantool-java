package org.tarantool;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;

import java.nio.channels.SocketChannel;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReferenceArray;
import java.util.concurrent.locks.LockSupport;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

public class ClientReconnectIT extends AbstractTarantoolConnectorIT {
    private static final String INSTANCE_NAME = "jdk-testing";
    private TarantoolClient client;

    @AfterEach
    public void tearDown() {
        if (client != null) {
            assertTimeoutPreemptively(RESTART_TIMEOUT, "Close is stuck.", new Runnable() {
                @Override
                public void run() {
                    client.close();
                }
            });
        }
    }

    @AfterAll
    public static void tearDownEnv() {
        // Re-open console for cleanupEnv() to work.
        console.close();
        console = openConsole();
    }

    @Test
    public void testReconnect() throws Exception {
        client = makeClient();

        client.syncOps().ping();

        stopTarantool(INSTANCE_NAME);

        Exception e = assertThrows(Exception.class, new Executable() {
            @Override
            public void execute() {
                client.syncOps().ping();
            }
        });

        assertTrue(CommunicationException.class.isAssignableFrom(e.getClass()) ||
            IllegalStateException.class.isAssignableFrom(e.getClass()));

        assertNotNull(((TarantoolClientImpl) client).getThumbstone());

        assertFalse(client.isAlive());

        startTarantool(INSTANCE_NAME);

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
        SocketChannelProvider provider = new SocketChannelProvider() {
            @Override
            public SocketChannel get(int retryNumber, Throwable lastError) {
                if (lastError == null) {
                    latch.countDown();
                }
                return socketChannelProvider.get(retryNumber, lastError);
            }
        };

        client = new TarantoolClientImpl(provider, makeClientConfig());
        client.syncOps().ping();

        // The park() will return inside connector thread.
        LockSupport.unpark(((TarantoolClientImpl)client).connector);

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
        client = new TarantoolClientImpl(socketChannelProvider, makeClientConfig()) {
            @Override
            protected void write(Code code, Long syncId, Long schemaId, Object... args) {
                // Skip write.
            }
        };

        final Future<List<?>> res = client.asyncOps().select(SPACE_ID, PK_INDEX_ID, Collections.singletonList(1),
            0, 1, Iterator.EQ);

        client.close();

        ExecutionException e = assertThrows(ExecutionException.class, new Executable() {
            @Override
            public void execute() throws Throwable {
                res.get();
            }
        });
        assertEquals("Connection is closed.", e.getCause().getMessage());
    }

    /**
     * When the reconnection happen, the outstanding operations must fail.
     * Otherwise, synchronous wait on such operations will block forever.
     */
    @Test
    public void testReconnectWhileOperationsAreInProgress() {
        final AtomicBoolean writeEnabled = new AtomicBoolean(false);
        client = new TarantoolClientImpl(socketChannelProvider, makeClientConfig()) {
            @Override
            protected void write(Code code, Long syncId, Long schemaId, Object... args) throws Exception {
                if (writeEnabled.get()) {
                    super.write(code, syncId, schemaId, args);
                }
            }
        };

        final Future<List<?>> mustFail = client.asyncOps().select(SPACE_ID, PK_INDEX_ID, Collections.singletonList(1),
            0, 1, Iterator.EQ);

        stopTarantool(INSTANCE_NAME);

        assertThrows(ExecutionException.class, new Executable() {
            @Override
            public void execute() throws Throwable {
                mustFail.get();
            }
        });

        startTarantool(INSTANCE_NAME);

        writeEnabled.set(true);

        try {
            client.waitAlive(RESTART_TIMEOUT, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            fail();
        }

        Future<List<?>> res = client.asyncOps().select(SPACE_ID, PK_INDEX_ID, Collections.singletonList(1),
            0, 1, Iterator.EQ);

        try {
            res.get(TIMEOUT, TimeUnit.MILLISECONDS);
        } catch (Exception e) {
            fail(e);
        }
    }

    @Test
    public void testConcurrentCloseAndReconnect() {
        final CountDownLatch latch = new CountDownLatch(2);
        client = new TarantoolClientImpl(socketChannelProvider, makeClientConfig()) {
            @Override
            protected void connect(final SocketChannel channel) throws Exception {
                latch.countDown();
                super.connect(channel);
            }
        };

        stopTarantool(INSTANCE_NAME);
        startTarantool(INSTANCE_NAME);

        try {
            assertTrue(latch.await(RESTART_TIMEOUT, TimeUnit.MILLISECONDS));
        } catch (InterruptedException e) {
            fail(e);
        }

        assertTimeoutPreemptively(RESTART_TIMEOUT, "Close is stuck.", new Runnable() {
            @Override
            public void run() {
                client.close();
            }
        });
    }

    /**
     * Test concurrent operations, reconnects and close.
     * Expected situation is nothing gets stuck.
     */
    @Test
    public void testLongParallelCloseReconnects() {
        int numThreads = 4;
        int numClients = 4;
        int timeBudget = 30*1000;

        final AtomicReferenceArray<TarantoolClient> clients =
            new AtomicReferenceArray<TarantoolClient>(numClients);

        for (int idx = 0; idx < clients.length(); idx++) {
            clients.set(idx, makeClient());
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
                    while (!Thread.currentThread().isInterrupted() &&
                        deadline > System.currentTimeMillis()) {

                        int idx = rnd.nextInt(clients.length());

                        try {
                            TarantoolClient cli = clients.get(idx);

                            int maxOps = rnd.nextInt(100);
                            for (int n = 0; n < maxOps; n++) {
                                cli.syncOps().ping();
                            }

                            cli.close();

                            TarantoolClient next = makeClient();
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
            stopTarantool(INSTANCE_NAME);
            startTarantool(INSTANCE_NAME);
            try {
                Thread.sleep(RESTART_TIMEOUT * 2);
            } catch (InterruptedException e) {
                fail(e);
            }
            if (deadline > System.currentTimeMillis()) {
                System.out.println("" + (deadline - System.currentTimeMillis())/1000 + "s remains.");
            }
        }

        // Wait for all threads to finish.
        try {
            assertTrue(latch.await(RESTART_TIMEOUT, TimeUnit.MILLISECONDS));
        } catch (InterruptedException e) {
            fail(e);
        }

        // Close outstanding clients.
        for (int idx = 0; idx < clients.length(); idx++) {
            clients.get(idx).close();
        }

        assertTrue(cnt.get() > threads.length);
    }
}
