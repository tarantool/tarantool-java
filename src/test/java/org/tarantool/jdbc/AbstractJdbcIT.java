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

//mvn -DtntHost=localhost -DtntPort=3301 -DtntUser=test -DtntPass=test verify
public abstract class AbstractJdbcIT {
    private static final String host = System.getProperty("tntHost", "localhost");
    private static final Integer port = Integer.valueOf(System.getProperty("tntPort", "3301"));
    private static final String user = System.getProperty("tntUser", "test_admin");
    private static final String pass = System.getProperty("tntPass", "4pWBZmLEgkmKK5WP");
    private static String URL = String.format("tarantool://%s:%d?user=%s&password=%s", host, port, user, pass);

    private static String[] initSql = new String[] {
            "DROP TABLE IF EXISTS test",
            "DROP TABLE IF EXISTS test_types",

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
                    "129479994)" //TIMESTAMP
    };

    private static String[] cleanSql = new String[] {
            "DROP TABLE IF EXISTS test",
            "DROP TABLE IF EXISTS test_types"
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

    Connection conn;

    @BeforeAll
    public static void setupEnv() throws Exception {
        sqlExec(initSql);
    }

    @AfterAll
    public static void teardownEnv() throws Exception {
        sqlExec(cleanSql);
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

    private static void sqlExec(String[] text) throws IOException {
        Socket socket = new Socket();
        try {
            socket.connect(new InetSocketAddress(host, port));
            TarantoolConnection con = new TarantoolConnection(user, pass, socket);
            try {
                for (String cmd : text)
                    con.eval("box.sql.execute(\"" + cmd + "\")");
            }
            finally {
                con.close();
                socket = null;
            }
        }
        finally {
            if (socket != null)
                socket.close();
        }
    }

    static List<?> getRow(String space, Object key) throws IOException {
        Socket socket = new Socket();
        try {
            socket.connect(new InetSocketAddress(host, port));
            TarantoolConnection con = new TarantoolConnection(user, pass, socket);
            try {
                List<?> l = con.select(281, 2, Arrays.asList(space.toUpperCase()), 0, 1, 0);
                Integer spaceId = (Integer) ((List) l.get(0)).get(0);
                l = con.select(spaceId, 0, Arrays.asList(key), 0, 1, 0);
                return (l == null || l.size() == 0) ? Collections.emptyList() : (List<?>) l.get(0);
            }
            finally {
                con.close();
                socket = null;
            }
        }
        finally {
            if (socket != null)
                socket.close();
        }
    }
}
