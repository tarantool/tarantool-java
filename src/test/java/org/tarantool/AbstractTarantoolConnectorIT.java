package org.tarantool;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.opentest4j.AssertionFailedError;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import static org.tarantool.TestUtils.makeInstanceEnv;

/**
 * Abstract test. Provides environment control and frequently used functions.
 */
public abstract class AbstractTarantoolConnectorIT {
    protected static final String host = System.getProperty("tntHost", "localhost");
    protected static final int port = Integer.parseInt(System.getProperty("tntPort", "3301"));
    protected static final int consolePort = Integer.parseInt(System.getProperty("tntConsolePort", "3313"));
    protected static final String username = System.getProperty("tntUser", "test_admin");
    protected static final String password = System.getProperty("tntPass", "4pWBZmLEgkmKK5WP");

    protected static final String LUA_FILE = "jdk-testing.lua";
    protected static final int LISTEN = 3301;
    protected static final int ADMIN = 3313;
    protected static final int TIMEOUT = 500;
    protected static final int RESTART_TIMEOUT = 2000;

    protected static final SocketChannelProvider socketChannelProvider = new TestSocketChannelProvider(host, port,
        RESTART_TIMEOUT);

    protected static TarantoolControl control;
    protected static TarantoolConsole console;

    protected static final String SPACE_NAME = "basic_test";
    protected static final String MULTIPART_SPACE_NAME = "multipart_test";

    protected static int SPACE_ID;
    protected static int MULTI_PART_SPACE_ID;

    protected static int PK_INDEX_ID;
    protected static int MPK_INDEX_ID;
    protected static int VIDX_INDEX_ID;

    private static final String[] setupScript = new String[] {
        "box.schema.space.create('basic_test', { format = " +
            "{{name = 'id', type = 'integer'}," +
            " {name = 'val', type = 'string'} } })",

        "box.space.basic_test:create_index('pk', { type = 'TREE', parts = {'id'} } )",
        "box.space.basic_test:create_index('vidx', { type = 'TREE', unique = false, parts = {'val'} } )",

        "box.space.basic_test:replace{1, 'one'}",
        "box.space.basic_test:replace{2, 'two'}",
        "box.space.basic_test:replace{3, 'three'}",

        "box.schema.space.create('multipart_test', { format = " +
            "{{name = 'id1', type = 'integer'}," +
            " {name = 'id2', type = 'string'}," +
            " {name = 'val1', type = 'string'} } })",

        "box.space.multipart_test:create_index('pk', { type = 'TREE', parts = {'id1', 'id2'} })",
        "box.space.multipart_test:create_index('vidx', { type = 'TREE', unique = false, parts = {'val1'} })",

        "box.space.multipart_test:replace{1, 'one', 'o n e'}",
        "box.space.multipart_test:replace{2, 'two', 't w o'}",
        "box.space.multipart_test:replace{3, 'three', 't h r e e'}",

        "function echo(...) return ... end"
    };

    private static final String[] cleanScript = new String[] {
        "box.space.basic_test and box.space.basic_test:drop()",
        "box.space.multipart_test and box.space.multipart_test:drop()"
    };

    @BeforeAll
    public static void setupEnv() {
        control = new TarantoolControl();
        control.createInstance("jdk-testing", LUA_FILE, makeInstanceEnv(LISTEN, ADMIN));
        startTarantool("jdk-testing");

        console = openConsole();

        executeLua(cleanScript);
        executeLua(setupScript);

        SPACE_ID = console.eval("box.space.basic_test.id");
        PK_INDEX_ID = console.eval("box.space.basic_test.index.pk.id");
        VIDX_INDEX_ID = console.eval("box.space.basic_test.index.vidx.id");

        MULTI_PART_SPACE_ID = console.eval("box.space.multipart_test.id");
        MPK_INDEX_ID = console.eval("box.space.multipart_test.index.pk.id");
    }

    @AfterAll
    public static void cleanupEnv() {
        try {
            executeLua(cleanScript);

            console.close();
        } finally {
            stopTarantool("jdk-testing");
        }
    }

    private static void executeLua(String[] exprs) {
        for (String expr : exprs) {
            console.exec(expr);
        }
    }

    protected void checkTupleResult(Object res, List tuple) {
        assertNotNull(res);
        assertTrue(List.class.isAssignableFrom(res.getClass()));
        List list = (List)res;
        assertEquals(1, list.size());
        assertNotNull(list.get(0));
        assertTrue(List.class.isAssignableFrom(list.get(0).getClass()));
        assertEquals(tuple, list.get(0));
    }

    protected TarantoolClient makeClient() {
        return new TarantoolClientImpl(socketChannelProvider, makeClientConfig());
    }

    protected static TarantoolClientConfig makeClientConfig() {
        return fillClientConfig(new TarantoolClientConfig());
    }

    protected static TarantoolClusterClientConfig makeClusterClientConfig() {
        TarantoolClusterClientConfig config = fillClientConfig(new TarantoolClusterClientConfig());
        config.executor = null;
        config.operationExpiryTimeMillis = TIMEOUT;
        return config;
    }

    private static <T> T fillClientConfig(TarantoolClientConfig config) {
        config.username = username;
        config.password = password;
        config.initTimeoutMillis = RESTART_TIMEOUT;
        config.sharedBufferSize = 128;
        return (T)config;
    }

    protected static TarantoolConsole openConsole() {
        return TarantoolConsole.open(host, consolePort);
    }

    protected static TarantoolConsole openConsole(String instance) {
        return TarantoolConsole.open(control.tntCtlWorkDir, instance);
    }

    protected TarantoolConnection openConnection() {
        Socket socket = new Socket();
        try {
            socket.connect(new InetSocketAddress(host, port));
        } catch (IOException e) {
            throw new RuntimeException("Test failed due to invalid environment.", e);
        }
        try {
            return new TarantoolConnection(username, password, socket);
        } catch (Exception e) {
            try {
                socket.close();
            } catch (IOException ignored) {
                // No-op.
            }
            throw new RuntimeException(e);
        }
    }

    protected List<?> consoleSelect(String spaceName, Object key) {
        StringBuilder sb = new StringBuilder("box.space.");
        sb.append(spaceName);
        sb.append(":select{");
        if (List.class.isAssignableFrom(key.getClass())) {
            List parts = (List)key;
            for (int i = 0; i < parts.size(); i++) {
                if (i != 0)
                    sb.append(", ");
                Object k = parts.get(i);
                if (k.getClass().isAssignableFrom(String.class)) {
                    sb.append('\'');
                    sb.append(k);
                    sb.append('\'');
                } else {
                    sb.append(k);
                }
            }
        } else {
            sb.append(key);
        }
        sb.append("}");
        return console.eval(sb.toString());
    }

    protected static void stopTarantool(String instance) {
        control.stop(instance);
        control.waitStopped("jdk-testing");
    }

    protected static void startTarantool(String instance) {
        control.start(instance);
        control.waitStarted("jdk-testing");
    }

    /**
     * Asserts that execution of the Runnable completes before the given timeout is exceeded.
     *
     * @param timeout Timeout in ms.
     * @param message Error message.
     * @param r Runnable.
     */
    protected void assertTimeoutPreemptively(int timeout, String message, Runnable r) {
        ExecutorService executorService = Executors.newSingleThreadExecutor();

        Future future = executorService.submit(r);

        try {
            future.get(timeout, TimeUnit.MILLISECONDS);
        } catch (TimeoutException ex) {
            throw new AssertionFailedError(message);
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        } finally {
            executorService.shutdownNow();
        }
    }
}
