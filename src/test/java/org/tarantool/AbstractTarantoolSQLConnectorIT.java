package org.tarantool;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.tarantool.TestUtils.makeInstanceEnv;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Abstract test. Provides environment control and frequently used functions which are related to SQL.
 */
public abstract class AbstractTarantoolSQLConnectorIT {

    protected static final String HOST = System.getProperty("tntHost", "localhost");
    protected static final int PORT = Integer.parseInt(System.getProperty("tntPort", "3301"));
    protected static final int CONSOLE_PORT = Integer.parseInt(System.getProperty("tntConsolePort", "3313"));
    protected static final String USERNAME = System.getProperty("tntUser", "test_admin");
    protected static final String PASSWORD = System.getProperty("tntPass", "4pWBZmLEgkmKK5WP");

    protected static final String LUA_FILE = "jdk-testing.lua";
    protected static final int LISTEN = 3301;
    protected static final int ADMIN = 3313;
    protected static final int TIMEOUT = 500;
    protected static final int RESTART_TIMEOUT = 2000;

    protected static final SocketChannelProvider socketChannelProvider = new TestSocketChannelProvider(
        HOST, PORT, RESTART_TIMEOUT
    );

    protected static TarantoolControl control;
    protected static TarantoolConsole console;

    protected static final String TABLE_NAME = "sql_test";

    private static final String[] setupScript = new String[] {
        "\\set language sql",
        "\\set delimiter ;",

        "CREATE TABLE sql_test (id INTEGER PRIMARY KEY, val VARCHAR(100));",
        "CREATE UNIQUE INDEX sql_test_val_index_unique ON sql_test (val);",

        "INSERT INTO sql_test VALUES (1, 'A');",
        "INSERT INTO sql_test VALUES (2, 'B');",
        "INSERT INTO sql_test VALUES (3, 'C');",
    };

    private static final String[] cleanScript = new String[] {
        "DROP TABLE sql_test;"
    };

    @BeforeAll
    public static void setupEnv() {
        control = new TarantoolControl();
        control.createInstance("jdk-testing", LUA_FILE, makeInstanceEnv(LISTEN, ADMIN));
        startTarantool("jdk-testing");

        console = openConsole();

        executeLua(setupScript);
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

    protected void checkTupleResult(List<Map<String, Object>> expected, List<Map<String, Object>> actual) {
        assertNotNull(expected);
        assertEquals(expected, actual);
    }

    protected List<Map<String, Object>> asResult(Object[][] tuples) {
        List<Map<String, Object>> result = new ArrayList<>();
        if (tuples != null) {
            for (int i = 0; i < tuples.length; i++) {
                Object[] tuple = tuples[i];
                if (tuple.length % 2 != 0) {
                    continue;
                }
                Map<String, Object> row = new HashMap<>();
                for (int j = 0; j <= tuple.length / 2; j += 2) {
                    row.put(tuple[j].toString(), tuple[j + 1]);
                }
                result.add(row);
            }
        }
        return result;
    }

    protected TarantoolClient makeClient() {
        return new TarantoolClientImpl(socketChannelProvider, makeClientConfig());
    }

    protected TarantoolClient makeClient(SocketChannelProvider provider) {
        return new TarantoolClientImpl(provider, makeClientConfig());
    }

    protected static TarantoolClientConfig makeClientConfig() {
        TarantoolClientConfig config = new TarantoolClientConfig();
        config.username = USERNAME;
        config.password = PASSWORD;
        config.initTimeoutMillis = RESTART_TIMEOUT;
        config.sharedBufferSize = 128;
        return config;
    }

    protected static TarantoolConsole openConsole() {
        return TarantoolConsole.open(HOST, CONSOLE_PORT);
    }

    protected static TarantoolConsole openConsole(String instance) {
        return TarantoolConsole.open(control.tntCtlWorkDir, instance);
    }

    protected static void stopTarantool(String instance) {
        control.stop(instance);
    }

    protected static void startTarantool(String instance) {
        control.start(instance);
    }

}
