package org.tarantool.jdbc;

import static org.tarantool.TestAssumptions.assumeMinimalServerVersion;
import static org.tarantool.TestUtils.makeInstanceEnv;
import static org.tarantool.jdbc.SqlTestUtils.getCreateTableSQL;

import org.tarantool.ServerVersion;
import org.tarantool.TarantoolClientConfig;
import org.tarantool.TarantoolClientImpl;
import org.tarantool.TarantoolConsole;
import org.tarantool.TarantoolControl;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

//mvn -DtntHost=localhost -DtntPort=3301 -DtntUser=test -DtntPass=test verify
public abstract class AbstractJdbcIT {

    private static final String host = System.getProperty("tntHost", "localhost");
    private static final Integer port = Integer.valueOf(System.getProperty("tntPort", "3301"));
    private static final String user = System.getProperty("tntUser", "test_admin");
    private static final String pass = System.getProperty("tntPass", "4pWBZmLEgkmKK5WP");
    private static String URL = String.format("jdbc:tarantool://%s:%d?user=%s&password=%s", host, port, user, pass);

    protected static final String LUA_FILE = "jdk-testing.lua";
    protected static final int LISTEN = 3301;
    protected static final int ADMIN = 3313;

    private static String[] initSql = new String[] {
        "CREATE TABLE test(id INT PRIMARY KEY, val VARCHAR(100))",
        "INSERT INTO test VALUES (1, 'one'), (2, 'two'), (3, 'three')",
        "CREATE TABLE test_compound(id1 INT, id2 INT, val VARCHAR(100), PRIMARY KEY (id2, id1))",
        "CREATE TABLE test_nulls(id INT PRIMARY KEY, val VARCHAR(100), dig INTEGER, bin SCALAR)",
        "INSERT INTO test_nulls VALUES (1, 'a', 10, 'aa'), (2, 'b', 20, 'bb'), (3, 'c', 30, 'cc'), " +
            "(4, NULL, NULL, NULL), (5, NULL, NULL, NULL), (6, NULL, NULL, NULL)",
        getCreateTableSQL("test_types", TntSqlType.values())
    };

    private static String[] cleanSql = new String[] {
        "DROP TABLE IF EXISTS test",
        "DROP TABLE IF EXISTS test_types",
        "DROP TABLE IF EXISTS test_compound",
        "DROP TABLE IF EXISTS test_nulls"
    };

    protected static TarantoolControl control;
    Connection conn;

    @BeforeAll
    public static void setupEnv() {
        control = new TarantoolControl();
        control.createInstance("jdk-testing", LUA_FILE, makeInstanceEnv(LISTEN, ADMIN));
        control.start("jdk-testing");
    }

    @AfterAll
    public static void teardownEnv() {
        control.stop("jdk-testing");
    }

    @BeforeEach
    public void setUpTest() throws SQLException {
        assumeMinimalServerVersion(TarantoolConsole.open(host, ADMIN), ServerVersion.V_2_1);

        conn = DriverManager.getConnection(URL);
        sqlExec(cleanSql);
        sqlExec(initSql);
    }

    @AfterEach
    public void tearDownTest() throws SQLException {
        assumeMinimalServerVersion(TarantoolConsole.open(host, ADMIN), ServerVersion.V_2_1);
        sqlExec(cleanSql);
        if (conn != null && !conn.isClosed()) {
            conn.close();
        }
    }

    protected static void sqlExec(String... text) {
        TarantoolClientImpl client = makeClient();
        try {
            for (String cmd : text) {
                client.syncOps().eval("box.execute(\"" + cmd + "\")");
            }
        } finally {
            client.close();
        }
    }

    static List<?> getRow(String space, Object key) {
        TarantoolClientImpl client = makeClient();
        try {
            List<?> l = client.syncOps().select(281, 2, Arrays.asList(space.toUpperCase()), 0, 1, 0);
            Integer spaceId = (Integer) ((List) l.get(0)).get(0);
            l = client.syncOps().select(spaceId, 0, Arrays.asList(key), 0, 1, 0);
            return (l == null || l.size() == 0) ? Collections.emptyList() : (List<?>) l.get(0);
        } finally {
            client.close();
        }
    }

    static TarantoolClientImpl makeClient() {
        return new TarantoolClientImpl(host + ":" + port, makeClientConfig());
    }

    private static TarantoolClientConfig makeClientConfig() {
        TarantoolClientConfig config = new TarantoolClientConfig();
        config.username = user;
        config.password = pass;
        config.initTimeoutMillis = 2000;
        return config;
    }

}
