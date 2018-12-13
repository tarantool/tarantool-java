package org.tarantool.jdbc;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.tarantool.TarantoolConnection;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.tarantool.TestUtils.makeInstanceEnv;

import org.tarantool.TarantoolControl;

//mvn -DtntHost=localhost -DtntPort=3301 -DtntUser=test -DtntPass=test verify
public abstract class AbstractJdbcIT {
    private static final String host = System.getProperty("tntHost", "localhost");
    private static final Integer port = Integer.valueOf(System.getProperty("tntPort", "3301"));
    private static final String user = System.getProperty("tntUser", "test_admin");
    private static final String pass = System.getProperty("tntPass", "4pWBZmLEgkmKK5WP");
    private static String URL = String.format("tarantool://%s:%d?user=%s&password=%s", host, port, user, pass);

    protected static final String LUA_FILE = "jdk-testing.lua";
    protected static final int LISTEN = 3301;
    protected static final int ADMIN = 3313;

    private static String[] initSql = new String[] {
            "CREATE TABLE test(id INT PRIMARY KEY, val VARCHAR(100))",
            "INSERT INTO test VALUES (1, 'one'), (2, 'two'), (3, 'three')",

            "CREATE TABLE test_types(" +
                    "f1 INT PRIMARY KEY, " +
                    "f2 CHAR(4), " +
                    "f3 VARCHAR(100), " +
                    "f4 LONGVARCHAR(100), " +
                    "f5 NUMERIC, " +
                    "f6 DECIMAL, " +
                    "f7 BIT, " +
                    "f8 TINYINT, " +
                    "f9 SMALLINT, " +
                    "f10 INTEGER, " +
                    "f11 BIGINT," +
                    "f12 REAL, " +
                    "f13 FLOAT, " +
                    "f14 BINARY(4), " +
                    "f15 VARBINARY(128), " +
                    "f16 LONGVARBINARY(2048), " +
                    "f17 DATE, " +
                    "f18 TIME, " +
                    "f19 TIMESTAMP)",

            "INSERT INTO test_types VALUES(" +
                    "1," +
                    "'abcd'," + //CHAR
                    "'000000000000000000001'," + //VARCHAR
                    "'0000000000000000000000000000000001'," + //LONGVARCHAR
                    "100," + // NUMERIC
                    "100.1," + // DECIMAL
                    "1," + //BIT
                    "7," + //TINYINT
                    "1000," + //SMALLINT
                    "100," + //INTEGER
                    "100000000000000000," + //BIGINT
                    "-100.2," + //REAL
                    "100.3," + //FLOAT
                    "X'01020304'," + //BINARY
                    "X'0102030405'," +//VARBINARY
                    "X'010203040506'," + //LONGVARBINARY
                    "'1983-03-14'," + //DATE
                    "'12:01:06'," + //TIME
                    "129479994)", //TIMESTAMP

            "CREATE TABLE test_compound(id1 INT, id2 INT, val VARCHAR(100), PRIMARY KEY (id2, id1))"
    };

    private static String[] cleanSql = new String[] {
            "DROP TABLE IF EXISTS test",
            "DROP TABLE IF EXISTS test_types",
            "DROP TABLE IF EXISTS test_compound"
    };

    static Object[] testRow = new Object[] {
        1,
        "abcd",
        "000000000000000000001",
        "0000000000000000000000000000000001",
        BigDecimal.valueOf(100),
        BigDecimal.valueOf(100.1),
        Boolean.FALSE,
        (byte)7,
        (short)1000,
        100,
        100000000000000000L,
        -100.2f,
        100.3d,
        new BigInteger("01020304", 16).toByteArray(),
        new BigInteger("0102030405", 16).toByteArray(),
        new BigInteger("010203040506", 16).toByteArray(),
        Date.valueOf("1983-03-14"),
        Time.valueOf("12:01:06"),
        new Timestamp(129479994)
    };

    protected static TarantoolControl control;
    Connection conn;

    @BeforeAll
    public static void setupEnv() throws Exception {
        control = new TarantoolControl();
        control.createInstance("jdk-testing", LUA_FILE, makeInstanceEnv(LISTEN, ADMIN));
        control.start("jdk-testing");
        control.waitStarted("jdk-testing");

        sqlExec(cleanSql);
        sqlExec(initSql);
    }

    @AfterAll
    public static void teardownEnv() throws Exception {
        try {
            sqlExec(cleanSql);
        } finally {
            control.stop("jdk-testing");
            control.waitStopped("jdk-testing");
        }
    }

    @BeforeEach
    public void setUpConnection() throws SQLException {
        conn = DriverManager.getConnection(URL);
        assertNotNull(conn);
    }

    @AfterEach
    public void tearDownConnection() throws SQLException {
        if (conn != null && !conn.isClosed())
            conn.close();
    }

    private static void sqlExec(String[] text) {
        TarantoolConnection con = makeConnection();
        try {
            for (String cmd : text)
                con.eval("box.sql.execute(\"" + cmd + "\")");
        } finally {
            con.close();
        }
    }

    static List<?> getRow(String space, Object key) {
        TarantoolConnection con = makeConnection();
        try {
            List<?> l = con.select(281, 2, Arrays.asList(space.toUpperCase()), 0, 1, 0);
            Integer spaceId = (Integer) ((List) l.get(0)).get(0);
            l = con.select(spaceId, 0, Arrays.asList(key), 0, 1, 0);
            return (l == null || l.size() == 0) ? Collections.emptyList() : (List<?>) l.get(0);
        } finally {
            con.close();
        }
    }

    static TarantoolConnection makeConnection() {
        Socket socket = new Socket();
        try {
            socket.connect(new InetSocketAddress(host, port));
            return new TarantoolConnection(user, pass, socket);
        } catch (IOException e) {
            try {
                socket.close();
            } catch (IOException ignored) {
                // No-op.
            }
            throw new RuntimeException(e);
        }
    }
}
