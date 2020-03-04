package org.tarantool.jdbc;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.Mockito.anyInt;
import static org.mockito.Mockito.anyObject;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.tarantool.TestAssumptions.assumeMinimalServerVersion;

import org.tarantool.TarantoolTestHelper;
import org.tarantool.TestUtils;
import org.tarantool.util.SQLStates;
import org.tarantool.util.ServerVersion;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.io.StringReader;
import java.nio.charset.StandardCharsets;
import java.sql.BatchUpdateException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Statement;
import java.sql.Types;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class JdbcPreparedStatementIT {

    private static final String[] INIT_SQL = new String[] {
        "CREATE TABLE test(id INT PRIMARY KEY, val VARCHAR(100), bin_val SCALAR)",
    };

    private static final String[] CLEAN_SQL = new String[] {
        "DROP TABLE IF EXISTS test"
    };

    private static TarantoolTestHelper testHelper;
    private static Connection conn;

    private PreparedStatement prep;

    @BeforeAll
    public static void setupEnv() throws SQLException {
        testHelper = new TarantoolTestHelper("jdbc-prepared-it");
        testHelper.createInstance();
        testHelper.startInstance();

        conn = DriverManager.getConnection(SqlTestUtils.makeDefaultJdbcUrl());
    }

    @AfterAll
    public static void teardownEnv() throws SQLException {
        if (conn != null) {
            conn.close();
        }
        testHelper.stopInstance();
    }

    @BeforeEach
    public void setUpTest() throws SQLException {
        assumeMinimalServerVersion(testHelper.getInstanceVersion(), ServerVersion.V_2_1);
        testHelper.executeSql(INIT_SQL);
    }

    @AfterEach
    public void tearDownTest() throws SQLException {
        assumeMinimalServerVersion(testHelper.getInstanceVersion(), ServerVersion.V_2_1);
        testHelper.executeSql(CLEAN_SQL);

        if (prep != null) {
            prep.close();
        }
    }

    @Test
    public void testExecuteQuery() throws SQLException {
        testHelper.executeSql("INSERT INTO test(id, val) VALUES (1, 'one'), (2, 'two')");

        prep = conn.prepareStatement("SELECT val FROM test WHERE id = ?");
        assertNotNull(prep);

        prep.setInt(1, 1);
        ResultSet rs = prep.executeQuery();
        assertNotNull(rs);
        assertTrue(rs.next());
        assertEquals("one", rs.getString(1));
        assertFalse(rs.next());
        rs.close();

        // Reuse the prepared statement.
        prep.setInt(1, 2);
        rs = prep.executeQuery();
        assertNotNull(rs);
        assertTrue(rs.next());
        assertEquals("two", rs.getString(1));
        assertFalse(rs.next());
        rs.close();
    }

    @Test
    public void testExecuteWrongQuery() throws SQLException {
        prep = conn.prepareStatement("INSERT INTO test(id, val) VALUES (?, ?)");
        prep.setInt(1, 200);
        prep.setString(2, "two hundred");

        SQLException exception = assertThrows(SQLException.class, () -> prep.executeQuery());
        SqlAssertions.assertSqlExceptionHasStatus(exception, SQLStates.NO_DATA);
    }

    @Test
    public void testExecuteUpdate() throws Exception {
        prep = conn.prepareStatement("INSERT INTO test(id, val) VALUES(?, ?)");
        assertNotNull(prep);

        prep.setInt(1, 100);
        prep.setString(2, "hundred");
        int count = prep.executeUpdate();
        assertEquals(1, count);

        assertEquals("hundred", consoleSelect(100).get(1));

        // Reuse the prepared statement.
        prep.setInt(1, 1000);
        prep.setString(2, "thousand");
        count = prep.executeUpdate();
        assertEquals(1, count);

        assertEquals("thousand", consoleSelect(1000).get(1));
    }

    @Test
    public void testExecuteWrongUpdate() throws SQLException {
        prep = conn.prepareStatement("SELECT val FROM test WHERE id=?");
        prep.setInt(1, 1);

        SQLException exception = assertThrows(SQLException.class, () -> prep.executeUpdate());
        SqlAssertions.assertSqlExceptionHasStatus(exception, SQLStates.TOO_MANY_RESULTS);
    }

    @Test
    public void testExecuteReturnsResultSet() throws SQLException {
        testHelper.executeSql("INSERT INTO test(id, val) VALUES (1, 'one')");

        prep = conn.prepareStatement("SELECT val FROM test WHERE id=?");
        prep.setInt(1, 1);

        assertTrue(prep.execute());
        assertEquals(-1, prep.getUpdateCount());

        try (ResultSet resultSet = prep.getResultSet()) {
            assertNotNull(resultSet);
            assertTrue(resultSet.next());
            assertEquals("one", resultSet.getString(1));
            assertFalse(resultSet.next());
        }
    }

    @Test
    public void testExecuteReturnsUpdateCount() throws Exception {
        prep = conn.prepareStatement("INSERT INTO test(id, val) VALUES(?, ?), (?, ?)");
        assertNotNull(prep);

        prep.setInt(1, 10);
        prep.setString(2, "ten");
        prep.setInt(3, 20);
        prep.setString(4, "twenty");

        assertFalse(prep.execute());
        assertNull(prep.getResultSet());
        assertEquals(2, prep.getUpdateCount());

        assertEquals("ten", consoleSelect(10).get(1));
        assertEquals("twenty", consoleSelect(20).get(1));
    }

    @Test
    void testForbiddenMethods() throws SQLException {
        prep = conn.prepareStatement("TEST");

        int i = 0;
        for (; i < 3; i++) {
            final int step = i;
            SQLException e = assertThrows(SQLException.class, new Executable() {
                @Override
                public void execute() throws Throwable {
                    switch (step) {
                    case 0:
                        prep.executeQuery("TEST");
                        break;
                    case 1:
                        prep.executeUpdate("TEST");
                        break;
                    case 2:
                        prep.execute("TEST");
                        break;
                    default:
                        fail();
                    }
                }
            });
            assertEquals("The method cannot be called on a PreparedStatement.", e.getMessage());
        }
        assertEquals(3, i);
    }

    @Test
    public void testUnwrap() throws SQLException {
        prep = conn.prepareStatement("SELECT val FROM test");
        assertEquals(prep, prep.unwrap(SQLPreparedStatement.class));
        assertEquals(prep, prep.unwrap(SQLStatement.class));
        assertThrows(SQLException.class, () -> prep.unwrap(Integer.class));
    }

    @Test
    public void testIsWrapperFor() throws SQLException {
        prep = conn.prepareStatement("SELECT val FROM test");
        assertTrue(prep.isWrapperFor(SQLPreparedStatement.class));
        assertTrue(prep.isWrapperFor(SQLStatement.class));
        assertFalse(prep.isWrapperFor(Integer.class));
    }

    @Test
    public void testSupportGeneratedKeys() throws SQLException {
        prep = conn.prepareStatement("INSERT INTO test(id, val) values (50, 'fifty')", Statement.NO_GENERATED_KEYS);
        assertFalse(prep.execute());
        assertEquals(1, prep.getUpdateCount());

        ResultSet generatedKeys = prep.getGeneratedKeys();
        assertNotNull(generatedKeys);
        assertEquals(ResultSet.TYPE_FORWARD_ONLY, generatedKeys.getType());
        assertEquals(ResultSet.CONCUR_READ_ONLY, generatedKeys.getConcurrency());
    }

    @Test
    public void testExecuteReturnGeneratedKeys() throws SQLException {
        testHelper.executeSql("CREATE TABLE test_auto (id INT PRIMARY KEY AUTOINCREMENT)");

        prep = conn.prepareStatement("INSERT INTO test_auto VALUES (?)", Statement.RETURN_GENERATED_KEYS);
        prep.setNull(1, Types.INTEGER);
        prep.execute();

        assertEquals(1, prep.getUpdateCount());
        ResultSet generatedKeys = prep.getGeneratedKeys();
        assertTrue(generatedKeys.next());
        assertEquals(1, generatedKeys.getInt("generated_key"));

        testHelper.executeSql("DROP TABLE test_auto");
    }

    @Test
    public void testExecuteUpdateReturnGeneratedKeys() throws SQLException {
        testHelper.executeSql("CREATE TABLE test_auto (id INT PRIMARY KEY AUTOINCREMENT)");

        prep = conn.prepareStatement("INSERT INTO test_auto VALUES (?), (?)", Statement.RETURN_GENERATED_KEYS);
        prep.setNull(1, Types.INTEGER);
        prep.setNull(2, Types.INTEGER);
        prep.execute();

        assertEquals(2, prep.getUpdateCount());
        ResultSet generatedKeys = prep.getGeneratedKeys();
        assertTrue(generatedKeys.next());
        assertEquals(1, generatedKeys.getInt("generated_key"));
        assertTrue(generatedKeys.next());
        assertEquals(2, generatedKeys.getInt("generated_key"));

        testHelper.executeSql("DROP TABLE test_auto");
    }

    @Test
    public void testExecuteReturnEmptyGeneratedKeys() throws SQLException {
        testHelper.executeSql("CREATE TABLE test_auto (id INT PRIMARY KEY AUTOINCREMENT)");

        prep = conn.prepareStatement("INSERT INTO test_auto VALUES (?), (?)", Statement.RETURN_GENERATED_KEYS);
        prep.setInt(1, 10);
        prep.setInt(2, 20);
        prep.execute();

        assertEquals(2, prep.getUpdateCount());
        ResultSet generatedKeys = prep.getGeneratedKeys();
        assertFalse(generatedKeys.next());

        testHelper.executeSql("DROP TABLE test_auto");
    }

    @Test
    public void testExecuteReturnMixedGeneratedKeys() throws SQLException {
        testHelper.executeSql("CREATE TABLE test_auto (id INT PRIMARY KEY AUTOINCREMENT)");

        prep = conn.prepareStatement(
            "INSERT INTO test_auto VALUES (?), (?), (?), (?)", Statement.RETURN_GENERATED_KEYS
        );
        prep.setInt(1, 10);
        prep.setNull(2, Types.INTEGER);
        prep.setInt(3, 20);
        prep.setNull(4, Types.INTEGER);
        prep.execute();

        assertEquals(4, prep.getUpdateCount());
        ResultSet generatedKeys = prep.getGeneratedKeys();
        assertTrue(generatedKeys.next());
        assertEquals(11, generatedKeys.getInt("generated_key"));
        assertTrue(generatedKeys.next());
        assertEquals(21, generatedKeys.getInt("generated_key"));

        testHelper.executeSql("DROP TABLE test_auto");
    }

    @Test
    void testStatementConnection() throws SQLException {
        Statement statement = conn.prepareStatement("SELECT * FROM TEST");
        assertEquals(conn, statement.getConnection());
    }

    @Test
    public void testMoreResultsWithResultSet() throws SQLException {
        prep = conn.prepareStatement("SELECT val FROM test WHERE id = ?");
        prep.setInt(1, 1);

        prep.execute();
        ResultSet resultSet = prep.getResultSet();

        assertFalse(resultSet.isClosed());
        assertFalse(prep.getMoreResults());
        assertEquals(-1, prep.getUpdateCount());
        assertTrue(resultSet.isClosed());
    }

    @Test
    public void testMoreResultsWithUpdateCount() throws SQLException {
        prep = conn.prepareStatement("INSERT INTO test(id, val) VALUES (?, ?)");
        prep.setInt(1, 9);
        prep.setString(2, "nine");

        prep.execute();
        int updateCount = prep.getUpdateCount();

        assertEquals(1, prep.getUpdateCount());
        assertFalse(prep.getMoreResults());
        assertEquals(-1, prep.getUpdateCount());
    }

    @Test
    public void testMoreResultsButCloseCurrent() throws SQLException {
        prep = conn.prepareStatement("SELECT val FROM test WHERE id = ?");
        prep.setInt(1, 2);

        prep.execute();
        ResultSet resultSet = prep.getResultSet();

        assertFalse(resultSet.isClosed());
        assertFalse(prep.getMoreResults(Statement.CLOSE_CURRENT_RESULT));
        assertEquals(-1, prep.getUpdateCount());
        assertTrue(resultSet.isClosed());
    }

    @Test
    public void testMoreResultsButCloseAll() throws SQLException {
        prep = conn.prepareStatement("SELECT val FROM test WHERE id = ?");
        prep.setInt(1, 2);
        prep.execute();

        assertThrows(SQLFeatureNotSupportedException.class, () -> prep.getMoreResults(Statement.CLOSE_ALL_RESULTS));

        prep = conn.prepareStatement("INSERT INTO test(id, val) VALUES (?, ?)");
        prep.setInt(1, 21);
        prep.setString(2, "twenty one");
        prep.execute();

        assertEquals(1, prep.getUpdateCount());
        assertFalse(prep.getMoreResults(Statement.CLOSE_ALL_RESULTS));
        assertEquals(-1, prep.getUpdateCount());
    }

    @Test
    public void testMoreResultsButKeepCurrent() throws SQLException {
        prep = conn.prepareStatement("SELECT val FROM test WHERE id = ?");
        prep.setInt(1, 3);
        prep.execute();

        assertThrows(SQLFeatureNotSupportedException.class, () -> prep.getMoreResults(Statement.KEEP_CURRENT_RESULT));

        prep = conn.prepareStatement("INSERT INTO test(id, val) VALUES (?, ?)");
        prep.setInt(1, 22);
        prep.setString(2, "twenty two");
        prep.execute();

        assertEquals(1, prep.getUpdateCount());
        assertFalse(prep.getMoreResults(Statement.KEEP_CURRENT_RESULT));
        assertEquals(-1, prep.getUpdateCount());
    }

    @Test
    public void testExecuteOneBatchQuery() throws Exception {
        prep = conn.prepareStatement("INSERT INTO test(id, val) VALUES (?, ?)");

        prep.setInt(1, 1);
        prep.setString(2, "one");
        prep.addBatch();

        int[] updateCounts = prep.executeBatch();
        assertEquals(1, updateCounts.length);
        assertEquals(1, updateCounts[0]);

        assertEquals("one", consoleSelect(1).get(1));
    }

    @Test
    public void testExecuteZeroBatchQuery() throws Exception {
        prep = conn.prepareStatement("INSERT INTO test(id, val) VALUES (1, 'one')");
        int[] updateCounts = prep.executeBatch();
        assertEquals(0, updateCounts.length);
    }

    @Test
    public void testExecuteBatchQuery() throws Exception {
        prep = conn.prepareStatement("INSERT INTO test(id, val) VALUES (?, ?)");

        prep.setInt(1, 1);
        prep.setString(2, "one");
        prep.addBatch();

        prep.setInt(1, 2);
        prep.setString(2, "two");
        prep.addBatch();

        prep.setInt(1, 3);
        prep.setString(2, "three");
        prep.addBatch();

        int[] updateCounts = prep.executeBatch();
        assertEquals(3, updateCounts.length);
        assertEquals(1, updateCounts[0]);
        assertEquals(1, updateCounts[1]);
        assertEquals(1, updateCounts[2]);

        assertEquals("one", consoleSelect(1).get(1));
        assertEquals("two", consoleSelect(2).get(1));
        assertEquals("three", consoleSelect(3).get(1));
    }

    @Test
    public void testExecuteMultiBatchQuery() throws Exception {
        prep = conn.prepareStatement("INSERT INTO test(id, val) VALUES (?, ?), (?, ?)");

        prep.setInt(1, 1);
        prep.setString(2, "one");
        prep.setInt(3, 2);
        prep.setString(4, "two");
        prep.addBatch();

        prep.setInt(1, 3);
        prep.setString(2, "three");
        prep.setInt(3, 4);
        prep.setString(4, "four");
        prep.addBatch();

        int[] updateCounts = prep.executeBatch();
        assertEquals(2, updateCounts.length);
        assertEquals(2, updateCounts[0]);
        assertEquals(2, updateCounts[1]);

        assertEquals("one", consoleSelect(1).get(1));
        assertEquals("two", consoleSelect(2).get(1));
        assertEquals("three", consoleSelect(3).get(1));
        assertEquals("four", consoleSelect(4).get(1));
    }

    @Test
    public void testClearBatch() throws Exception {
        prep = conn.prepareStatement("INSERT INTO test(id, val) VALUES (?, ?)");

        prep.setInt(1, 1);
        prep.setString(2, "one");
        prep.addBatch();

        prep.setInt(1, 2);
        prep.setString(2, "two");
        prep.addBatch();

        prep.clearBatch();

        int[] updateCounts = prep.executeBatch();
        assertEquals(0, updateCounts.length);
    }

    @Test
    public void testExecuteZeroCountsBatchQuery() throws Exception {
        testHelper.executeSql("INSERT INTO test(id, val) VALUES (1, 'one')");

        prep = conn.prepareStatement("DELETE FROM test WHERE id = ?");

        prep.setInt(1, 1);
        prep.addBatch();

        prep.setInt(1, 2);
        prep.addBatch();

        int[] updateCounts = prep.executeBatch();
        assertEquals(2, updateCounts.length);
        assertEquals(1, updateCounts[0]);
        assertEquals(0, updateCounts[1]);
    }

    @Test
    public void testExecuteFailedBatchQuery() throws Exception {
        prep = conn.prepareStatement("INSERT INTO test(id, val) VALUES (?, ?)");

        prep.setInt(1, 6);
        prep.setString(2, "six");
        prep.addBatch();

        prep.setInt(1, 6);
        prep.setString(2, "six");
        prep.addBatch();

        prep.setInt(1, 9);
        prep.setString(2, "nine");
        prep.addBatch();

        BatchUpdateException exception = assertThrows(BatchUpdateException.class, () -> prep.executeBatch());
        int[] updateCounts = exception.getUpdateCounts();
        assertEquals(3, updateCounts.length);
        assertEquals(1, updateCounts[0]);
        assertEquals(Statement.EXECUTE_FAILED, updateCounts[1]);
        assertEquals(1, updateCounts[2]);

        assertEquals("six", consoleSelect(6).get(1));
        assertEquals("nine", consoleSelect(9).get(1));
    }

    @Test
    public void testExecuteResultSetBatchQuery() throws Exception {
        prep = conn.prepareStatement("SELECT * FROM test WHERE id > ?");
        prep.setInt(1, 0);
        prep.addBatch();

        assertThrows(SQLException.class, () -> prep.executeBatch());
    }

    @Test
    public void testExecuteStringBatchQuery() throws Exception {
        prep = conn.prepareStatement("INSERT INTO test(id, val) VALUES (?, ?)");
        assertThrows(SQLException.class, () -> prep.addBatch("INSERT INTO test(id, val) VALUES (1, 'one')"));
    }

    @Test
    void testPoolableStatus() throws SQLException {
        prep = conn.prepareStatement("SELECT val FROM test WHERE id = ?");
        assertTrue(prep.isPoolable());
        prep.setPoolable(false);
        assertFalse(prep.isPoolable());
    }

    @Test
    public void testSetAsciiStream() throws Exception {
        prep = conn.prepareStatement("INSERT INTO test(id, val) VALUES (?, ?)");
        prep.setInt(1, 1);
        InputStream asciiStream = new ByteArrayInputStream("one".getBytes("ASCII"));
        prep.setAsciiStream(2, asciiStream);

        assertFalse(prep.execute());
        assertEquals("one", consoleSelect(1).get(1));
    }

    @Test
    public void testSetAsciiLimitedStream() throws Exception {
        prep = conn.prepareStatement("INSERT INTO test(id, val) VALUES (?, ?)");
        prep.setInt(1, 1);
        InputStream asciiStream = new ByteArrayInputStream("one and two and even three".getBytes("ASCII"));
        prep.setAsciiStream(2, asciiStream, 3);

        assertFalse(prep.execute());
        assertEquals("one", consoleSelect(1).get(1));
    }

    @Test
    public void testSetNegativeAsciiStream() throws Exception {
        prep = conn.prepareStatement("INSERT INTO test(id, val) VALUES (?, ?)");
        prep.setInt(1, 1);
        InputStream asciiStream = new ByteArrayInputStream("one and two and even three".getBytes("ASCII"));
        SQLException error = assertThrows(SQLException.class, () -> prep.setAsciiStream(2, asciiStream, -10));
        assertEquals(SQLStates.INVALID_PARAMETER_VALUE.getSqlState(), error.getSQLState());
    }

    @Test
    public void testSetBadStream() throws Exception {
        prep = conn.prepareStatement("INSERT INTO test(id, val) VALUES (?, ?)");

        InputStream throwingStream = mock(InputStream.class);
        when(throwingStream.read(anyObject(), anyInt(), anyInt())).thenThrow(IOException.class);

        SQLException error = assertThrows(
            SQLException.class,
            () -> prep.setAsciiStream(2, throwingStream)
        );
        assertEquals(SQLStates.INVALID_PARAMETER_VALUE.getSqlState(), error.getSQLState());
        assertEquals(IOException.class, error.getCause().getClass());
    }

    @Test
    public void testSetUnicodeLimitedStream() throws Exception {
        prep = conn.prepareStatement("INSERT INTO test(id, val) VALUES (?, ?)");
        prep.setInt(1, 1);
        InputStream unicodeStream = new ByteArrayInputStream("zéro one два みっつ 四 Fünf".getBytes("UTF-8"));
        // zéro is 5 bytes length because é consists of tow bytes 0xC3 0xA9
        prep.setUnicodeStream(2, unicodeStream, 5);

        assertFalse(prep.execute());
        assertEquals("zéro", consoleSelect(1).get(1));
    }

    @Test
    public void testSetNegativeUnicodeStream() throws Exception {
        prep = conn.prepareStatement("INSERT INTO test(id, val) VALUES (?, ?)");
        prep.setInt(1, 1);
        InputStream unicodeStream = new ByteArrayInputStream("one and two and even three".getBytes("UTF-8"));
        SQLException error = assertThrows(SQLException.class, () -> prep.setUnicodeStream(2, unicodeStream, -9));
        assertEquals(SQLStates.INVALID_PARAMETER_VALUE.getSqlState(), error.getSQLState());
    }

    @Test
    public void testSetBinaryStream() throws Exception {
        prep = conn.prepareStatement("INSERT INTO test(id, bin_val) VALUES (?, ?)");
        prep.setInt(1, 1);
        byte[] bytes = TestUtils.fromHex("00010203");
        prep.setBinaryStream(2, new ByteArrayInputStream(bytes));

        assertFalse(prep.execute());
        assertArrayEquals(bytes, ((String) consoleSelect(1).get(2)).getBytes(StandardCharsets.US_ASCII));
    }

    @Test
    public void testSetBinaryLimitedStream() throws Exception {
        prep = conn.prepareStatement("INSERT INTO test(id, bin_val) VALUES (?, ?)");
        prep.setInt(1, 1);
        byte[] bytes = TestUtils.fromHex("00010203040506");
        prep.setBinaryStream(2, new ByteArrayInputStream(bytes), 2);

        assertFalse(prep.execute());
        assertArrayEquals(
            Arrays.copyOf(bytes, 2),
            ((String) consoleSelect(1).get(2)).getBytes(StandardCharsets.US_ASCII)
        );
    }

    @Test
    public void testSetNegativeBinaryStream() throws Exception {
        prep = conn.prepareStatement("INSERT INTO test(id, bin_val) VALUES (?, ?)");
        byte[] bytes = TestUtils.fromHex("00010203040506");
        SQLException error = assertThrows(
            SQLException.class,
            () -> prep.setBinaryStream(2, new ByteArrayInputStream(bytes), -4)
        );
        assertEquals(SQLStates.INVALID_PARAMETER_VALUE.getSqlState(), error.getSQLState());
    }

    @Test
    public void testSetCharacterStream() throws Exception {
        prep = conn.prepareStatement("INSERT INTO test(id, val) VALUES (?, ?)");
        prep.setInt(1, 2);
        prep.setCharacterStream(2, new StringReader("two"));

        assertFalse(prep.execute());
        assertEquals("two", consoleSelect(2).get(1));
    }

    @Test
    public void testSetCharacterLimitedStream() throws Exception {
        prep = conn.prepareStatement("INSERT INTO test(id, val) VALUES (?, ?)");
        prep.setInt(1, 2);
        prep.setCharacterStream(2, new StringReader("two or maybe four"), 3);

        assertFalse(prep.execute());
        assertEquals("two", consoleSelect(2).get(1));
    }

    @Test
    public void testSetNegativeCharacterStream() throws Exception {
        prep = conn.prepareStatement("INSERT INTO test(id, val) VALUES (?, ?)");
        SQLException error = assertThrows(
            SQLException.class,
            () -> prep.setCharacterStream(2, new StringReader("four"), -10)
        );
        assertEquals(SQLStates.INVALID_PARAMETER_VALUE.getSqlState(), error.getSQLState());
    }

    @Test
    public void testSetBadCharacterStream() throws Exception {
        prep = conn.prepareStatement("INSERT INTO test(id, val) VALUES (?, ?)");

        Reader throwingReader = mock(Reader.class);
        when(throwingReader.read(anyObject(), anyInt(), anyInt())).thenThrow(IOException.class);

        SQLException error = assertThrows(
            SQLException.class,
            () -> prep.setCharacterStream(2, throwingReader)
        );
        assertEquals(SQLStates.INVALID_PARAMETER_VALUE.getSqlState(), error.getSQLState());
    }

    @Test
    public void testDisabledEscapeSyntax() throws Exception {
        testHelper.executeSql("INSERT INTO test(id, val) VALUES (1, 'one'), (2, 'two'), (3, 'three'), (4, 'four')");

        prep = conn.prepareStatement("SELECT val FROM test ORDER BY id {limit ?}");
        // according to JDBC API this call has no effect on escape processing
        // for prepared statements
        prep.setEscapeProcessing(false);

        prep.setInt(1, 1);
        prep.execute();

        try (ResultSet resultSet = prep.getResultSet()) {
            assertTrue(resultSet.next());
            assertEquals("one", resultSet.getString(1));
            assertFalse(resultSet.next());
        }

    }

    @Test
    public void testLimitEscapeSyntax() throws Exception {
        testHelper.executeSql("INSERT INTO test(id, val) VALUES (1, 'one'), (2, 'two'), (3, 'three'), (4, 'four')");

        prep = conn.prepareStatement("SELECT val FROM test ORDER BY id {limit ? offset ?}");
        prep.setInt(1, 2);
        prep.setInt(2, 0);

        prep.execute();

        try (ResultSet resultSet = prep.getResultSet()) {
            assertTrue(resultSet.next());
            assertEquals("one", resultSet.getString(1));
            assertTrue(resultSet.next());
            assertEquals("two", resultSet.getString(1));
            assertFalse(resultSet.next());
        }
    }

    @Test
    public void testLikeEscapeSyntax() throws Exception {
        testHelper.executeSql("INSERT INTO test(id, val) VALUES (1, 'one%'), (2, 'two'), (3, 'three%'), (4, 'four')");

        prep = conn.prepareStatement("SELECT val FROM test WHERE val LIKE '%|%' {escape ?}");
        prep.setString(1, "|");

        prep.execute();

        try (ResultSet resultSet = prep.getResultSet()) {
            assertTrue(resultSet.next());
            assertEquals("one%", resultSet.getString(1));
            assertTrue(resultSet.next());
            assertEquals("three%", resultSet.getString(1));
            assertFalse(resultSet.next());
        }
    }

    @Test
    public void testOuterJoinEscapeSyntax() throws Exception {
        testHelper.executeSql("INSERT INTO test(id, val) VALUES (1, 'one')");

        prep = conn.prepareStatement(
            "SELECT {fn concat('t1-', t1.val)}, {fn concat('t2-', t2.val)} " +
                "FROM {oj test t1 LEFT OUTER JOIN test t2 ON t1.id = ?}"
        );
        prep.setInt(1, 1);

        prep.execute();

        try (ResultSet resultSet = prep.getResultSet()) {
            assertTrue(resultSet.next());
            assertEquals("t1-one", resultSet.getString(1));
            assertEquals("t2-one", resultSet.getString(2));
            assertFalse(resultSet.next());
        }
    }

    @Test
    public void testSystemFunctionEscapeSyntax() throws Exception {
        testHelper.executeSql("INSERT INTO test(id, val) VALUES (1, NULL)");

        prep = conn.prepareStatement("SELECT {fn ifnull(val, ?)} FROM test WHERE id = 1");
        prep.setString(1, "one-one");

        prep.execute();

        try (ResultSet resultSet = prep.getResultSet()) {
            assertTrue(resultSet.next());
            assertEquals("one-one", resultSet.getString(1));
            assertFalse(resultSet.next());
        }
    }

    @Test
    public void testNumericFunctionEscapeSyntax() throws Exception {
        testHelper.executeSql("INSERT INTO test(id, val) VALUES (1, NULL)");

        prep = conn.prepareStatement("SELECT {fn abs(5 - ?)}, {fn round({fn pi()}, ?)}");
        prep.setInt(1, 10);
        prep.setInt(2, 0);

        prep.execute();

        try (ResultSet resultSet = prep.getResultSet()) {
            assertTrue(resultSet.next());
            assertEquals(5, resultSet.getInt(1));
            assertEquals(3, resultSet.getInt(2));
            assertFalse(resultSet.next());
        }
    }

    @Test
    public void testStringFunctionEscapeSyntax() throws Exception {
        testHelper.executeSql("INSERT INTO test(id, val) VALUES (1, 'one'), (2, 'TWO'), (3, 'three'), (4, ' four ')");

        prep = conn.prepareStatement(
            "SELECT {fn char(?)}, " +
                "{fn ucase(val)}, " +
                "{fn right(val, 2)}, " +
                "{fn concat(?, val)} " +
                "FROM test WHERE id = 3"
        );
        prep.setInt(1, 0x20);
        prep.setString(2, "3 ");

        prep.execute();

        try (ResultSet resultSet = prep.getResultSet()) {
            assertTrue(resultSet.next());
            assertEquals(" ", resultSet.getString(1));
            assertEquals("THREE", resultSet.getString(2));
            assertEquals("ee", resultSet.getString(3));
            assertEquals("3 three", resultSet.getString(4));
            assertFalse(resultSet.next());
        }
        prep.close();

        prep = conn.prepareStatement(
            "SELECT {fn lcase(val)}, " +
                "{fn left(val, ?)}, " +
                "{fn replace({fn lcase(val)}, 'two', ?)}, " +
                "{fn substring(val, ?, 2)} " +
                "FROM test WHERE id = 2"
        );
        prep.setInt(1, 2);
        prep.setString(2, "2");
        prep.setInt(3, 1);

        prep.execute();

        try (ResultSet resultSet = prep.getResultSet()) {
            assertTrue(resultSet.next());
            assertEquals("two", resultSet.getString(1));
            assertEquals("TW", resultSet.getString(2));
            assertEquals("2", resultSet.getString(3));
            assertEquals("TW", resultSet.getString(4));
            assertFalse(resultSet.next());
        }

        prep = conn.prepareStatement(
            "SELECT {fn rtrim(val)}, " +
                "{fn ltrim(val)} " +
                "FROM test WHERE id = ?"
        );
        prep.setInt(1, 4);

        prep.execute();

        try (ResultSet resultSet = prep.getResultSet()) {
            assertTrue(resultSet.next());
            assertEquals(" four", resultSet.getString(1));
            assertEquals("four ", resultSet.getString(2));
            assertFalse(resultSet.next());
        }
    }

    /**
     * Test length and soundex functions
     * which became available since 2.2.0
     */
    @Test
    void testStringFunctionFrom22() throws SQLException {
        assumeMinimalServerVersion(testHelper.getInstanceVersion(), ServerVersion.V_2_2);

        testHelper.executeSql("INSERT INTO test(id, val) VALUES (1, ' one ')");

        prep = conn.prepareStatement(
            "SELECT {fn length(val)}, {fn soundex(val)} FROM test"
        );

        prep.execute();

        try (ResultSet resultSet = prep.getResultSet()) {
            assertTrue(resultSet.next());
            assertEquals(4, resultSet.getInt(1));
            assertEquals("O500", resultSet.getString(2));
            assertFalse(resultSet.next());
        }
    }


    private List<?> consoleSelect(Object key) {
        List<?> list = testHelper.evaluate(TestUtils.toLuaSelect("TEST", key));
        return list == null ? Collections.emptyList() : (List<?>) list.get(0);
    }

}
