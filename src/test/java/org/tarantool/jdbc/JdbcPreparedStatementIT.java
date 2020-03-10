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
import static org.tarantool.TestAssumptions.assumeServerVersionLessThan;

import org.tarantool.TarantoolException;
import org.tarantool.TarantoolTestHelper;
import org.tarantool.TarantoolThreadDaemonFactory;
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
import java.sql.JDBCType;
import java.sql.ParameterMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Statement;
import java.sql.Types;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

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
        prep = conn.prepareStatement("SELECT 2");

        int i = 0;
        for (; i < 3; i++) {
            final int step = i;
            SQLException e = assertThrows(SQLException.class, new Executable() {
                @Override
                public void execute() throws Throwable {
                    switch (step) {
                    case 0:
                        prep.executeQuery("SELECT 1");
                        break;
                    case 1:
                        prep.executeUpdate("SELECT 2");
                        break;
                    case 2:
                        prep.execute("SELECT 3");
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
        prep = conn.prepareStatement("SELECT * FROM TEST");
        assertEquals(conn, prep.getConnection());
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
        try (PreparedStatement statement = conn.prepareStatement("SELECT val FROM test WHERE id = ?")) {
            statement.setInt(1, 2);
            statement.execute();
            assertThrows(
                SQLFeatureNotSupportedException.class,
                () -> statement.getMoreResults(Statement.CLOSE_ALL_RESULTS)
            );
        }

        try (PreparedStatement statement = conn.prepareStatement("INSERT INTO test(id, val) VALUES (?, ?)")) {
            statement.setInt(1, 21);
            statement.setString(2, "twenty one");
            statement.execute();

            assertEquals(1, statement.getUpdateCount());
            assertFalse(statement.getMoreResults(Statement.CLOSE_ALL_RESULTS));
            assertEquals(-1, statement.getUpdateCount());
        }
    }

    @Test
    public void testMoreResultsButKeepCurrent() throws SQLException {
        try (PreparedStatement statement = conn.prepareStatement("SELECT val FROM test WHERE id = ?")) {
            statement.setInt(1, 3);
            statement.execute();
            assertThrows(
                SQLFeatureNotSupportedException.class,
                () -> statement.getMoreResults(Statement.KEEP_CURRENT_RESULT)
            );
        }

        try (PreparedStatement statement = conn.prepareStatement("INSERT INTO test(id, val) VALUES (?, ?)")) {
            statement.setInt(1, 22);
            statement.setString(2, "twenty two");
            statement.execute();

            assertEquals(1, statement.getUpdateCount());
            assertFalse(statement.getMoreResults(Statement.KEEP_CURRENT_RESULT));
            assertEquals(-1, statement.getUpdateCount());
        }
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
    public void testGetAllColumnsMetadata() throws SQLException {
        assumeMinimalServerVersion(testHelper.getInstanceVersion(), ServerVersion.V_2_3);
        prep = conn.prepareStatement("SELECT * FROM test");
        ResultSetMetaData metaData = prep.getMetaData();

        assertEquals(3, metaData.getColumnCount());
        assertEquals("ID", metaData.getColumnName(1));
        assertEquals(JDBCType.BIGINT.getVendorTypeNumber(), metaData.getColumnType(1));

        assertEquals("VAL", metaData.getColumnName(2));
        assertEquals(JDBCType.VARCHAR.getVendorTypeNumber(), metaData.getColumnType(2));

        assertEquals("BIN_VAL", metaData.getColumnName(3));
        assertEquals(JDBCType.BINARY.getVendorTypeNumber(), metaData.getColumnType(3));
    }

    @Test
    public void testGetSpecifiedColumnMetadata() throws SQLException {
        assumeMinimalServerVersion(testHelper.getInstanceVersion(), ServerVersion.V_2_3);
        prep = conn.prepareStatement("SELECT val FROM test WHERE val = ?");
        ResultSetMetaData metaData = prep.getMetaData();

        assertEquals(1, metaData.getColumnCount());

        assertEquals("VAL", metaData.getColumnName(1));
        assertEquals(JDBCType.VARCHAR.getVendorTypeNumber(), metaData.getColumnType(1));
    }

    @Test
    public void testGetNullMetaDataWhenDml() throws SQLException {
        assumeMinimalServerVersion(testHelper.getInstanceVersion(), ServerVersion.V_2_3);
        prep = conn.prepareStatement("INSERT INTO test(id, val) VALUES (?, ?)");
        ResultSetMetaData metaData = prep.getMetaData();
        assertNull(metaData);
    }

    @Test
    public void testGetMetaDataAfterQueryExecution() throws SQLException {
        assumeMinimalServerVersion(testHelper.getInstanceVersion(), ServerVersion.V_2_3);
        testHelper.executeSql("INSERT INTO test(id, val) VALUES (1, 'one')");

        prep = conn.prepareStatement("SELECT val FROM test WHERE val = ?");
        ResultSetMetaData metaDataBefore = prep.getMetaData();
        prep.setInt(1, 1);
        prep.execute();
        ResultSetMetaData metaDataAfter = prep.getMetaData();

        assertEquals(metaDataBefore.getColumnCount(), metaDataAfter.getColumnCount());
        assertEquals(metaDataBefore.getColumnName(1), metaDataAfter.getColumnName(1));
        assertEquals(metaDataBefore.getColumnType(1), metaDataAfter.getColumnType(1));
    }

    @Test
    public void testUnsupportedPreparedStatement() throws SQLException {
        assumeServerVersionLessThan(testHelper.getInstanceVersion(), ServerVersion.V_2_3);
        prep = conn.prepareStatement("SELECT val FROM test");
        assertThrows(SQLException.class, () -> prep.getMetaData());
    }

    @Test
    public void testCachePreparedStatementsCount() throws SQLException {
        assumeMinimalServerVersion(testHelper.getInstanceVersion(), ServerVersion.V_2_3);
        final String cacheCountExpression = "box.info:sql().cache.stmt_count";

        int preparedCount = testHelper.evaluate(cacheCountExpression);
        assertEquals(0, preparedCount);
        try (
            PreparedStatement statement1 = conn.prepareStatement("INSERT INTO test (id, val) VALUES (?, ?)");
            PreparedStatement statement2 = conn.prepareStatement("SELECT val FROM test")
        ) {
            preparedCount = testHelper.evaluate(cacheCountExpression);
            assertEquals(2, preparedCount);
        }
        preparedCount = testHelper.evaluate(cacheCountExpression);
        assertEquals(0, preparedCount);
    }

    @Test
    public void testCachePreparedDuplicates() throws SQLException {
        assumeMinimalServerVersion(testHelper.getInstanceVersion(), ServerVersion.V_2_3);
        final String cacheCountExpression = "box.info:sql().cache.stmt_count";

        int preparedCount = testHelper.evaluate(cacheCountExpression);
        assertEquals(0, preparedCount);
        try (
            PreparedStatement statement1 = conn.prepareStatement("SELECT val FROM test");
            PreparedStatement statement2 = conn.prepareStatement("SELECT val FROM test")
        ) {
            preparedCount = testHelper.evaluate(cacheCountExpression);
            assertEquals(1, preparedCount);
        }
        preparedCount = testHelper.evaluate(cacheCountExpression);
        assertEquals(0, preparedCount);
    }

    @Test
    public void testSharePreparedStatementsPerSession() {
        assumeMinimalServerVersion(testHelper.getInstanceVersion(), ServerVersion.V_2_3);

        int threadsNumber = 16;
        int iterations = 100;
        final CountDownLatch latch = new CountDownLatch(threadsNumber);
        ExecutorService executor = Executors.newFixedThreadPool(
            threadsNumber,
            new TarantoolThreadDaemonFactory("shared-statements")
        );

        // multiple threads can prepare/deallocate same prepared statements simultaneously
        // using same connection. Tarantool does not count references of same SQL in scope
        // of one session that leads the driver should deal with it on its side and
        // deallocate after the last duplicate is released
        for (int i = 0; i < threadsNumber; i++) {
            executor.submit(() -> {
                try {
                    for (int k = 0; k < iterations; k++) {
                        try (
                            PreparedStatement statement1 = conn.prepareStatement("SELECT 1;");
                            PreparedStatement statement2 = conn.prepareStatement("SELECT 1;")
                        ) {
                            statement1.execute();
                            statement2.execute();
                        }
                        try (
                            PreparedStatement statement1 = conn.prepareStatement("SELECT 1;");
                            PreparedStatement statement2 = conn.prepareStatement("SELECT 2;")
                        ) {
                            statement1.execute();
                            statement2.execute();
                        }
                        try (PreparedStatement statement = conn.prepareStatement("SELECT 1;")) {
                            statement.execute();
                        }
                        try (PreparedStatement statement = conn.prepareStatement("SELECT 2;")) {
                            statement.execute();
                        }

                    }
                } catch (Exception ignored) {
                    return;
                }
                latch.countDown();
            });
        }

        try {
            assertTrue(latch.await(20, TimeUnit.SECONDS));
            int preparedCount = testHelper.evaluate("box.info:sql().cache.stmt_count");
            assertEquals(0, preparedCount);
        } catch (InterruptedException e) {
            fail(e);
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    public void testOutOfMemoryWhenPrepareStatement() throws SQLException {
        assumeMinimalServerVersion(testHelper.getInstanceVersion(), ServerVersion.V_2_3);
        final int oldSize = testHelper.evaluate("box.cfg.sql_cache_size");

        // emulate out of memory turning a prepered cache off
        testHelper.executeLua("box.cfg{sql_cache_size=0}");
        SQLException error = assertThrows(
            SQLException.class,
            () -> conn.prepareStatement("SELECT val FROM test")
        );

        assertTrue(error.getMessage().startsWith("Failed to execute SQL"));
        assertTrue(error.getCause() instanceof TarantoolException);
        assertTrue(error.getCause().getMessage().startsWith("Failed to prepare SQL"));

        testHelper.executeLua("box.cfg{sql_cache_size= " + oldSize + "}");
    }

    @Test
    public void testExpirePreparedStatementAfterDdl() throws SQLException {
        assumeMinimalServerVersion(testHelper.getInstanceVersion(), ServerVersion.V_2_3);
        try (
            PreparedStatement statement = conn.prepareStatement("INSERT INTO test (id, val) VALUES (?, ?)");
        ) {
            statement.setInt(1, 1);
            statement.setString(2, "one");
            assertEquals(1, statement.executeUpdate());

            testHelper.executeSql(
                "CREATE TABLE another_test (id INT PRIMARY KEY)",
                "DROP TABLE another_test"
            );

            statement.setInt(1, 2);
            statement.setString(2, "two");
            SQLException error = assertThrows(SQLException.class, statement::executeUpdate);
            assertTrue(error.getCause().getMessage().contains("statement has expired"));
        }
    }

    @Test
    public void testDeallocateExpiredPreparedStatement() throws SQLException {
        assumeMinimalServerVersion(testHelper.getInstanceVersion(), ServerVersion.V_2_3);
        try (
            PreparedStatement statement = conn.prepareStatement("SELECT 2;");
        ) {
            assertTrue(statement.execute());
            testHelper.executeSql(
                "CREATE TABLE another_test (id INT PRIMARY KEY)",
                "DROP TABLE another_test"
            );
        }
        int preparedCount = testHelper.evaluate("box.info:sql().cache.stmt_count");
        assertEquals(0, preparedCount);
    }

    @Test
    public void testPreparedParameterMetaData() throws SQLException {
        assumeMinimalServerVersion(testHelper.getInstanceVersion(), ServerVersion.V_2_3);
        prep = conn.prepareStatement("SELECT val FROM test WHERE id = ? AND val = ?");
        ParameterMetaData parameterMetaData = prep.getParameterMetaData();
        assertNotNull(parameterMetaData);
        assertEquals(2, parameterMetaData.getParameterCount());
    }

    @Test
    public void testEmptyParameterMetaData() throws SQLException {
        assumeMinimalServerVersion(testHelper.getInstanceVersion(), ServerVersion.V_2_3);
        prep = conn.prepareStatement("SELECT * FROM test");
        ParameterMetaData parameterMetaData = prep.getParameterMetaData();
        assertNotNull(parameterMetaData);
        assertEquals(0, parameterMetaData.getParameterCount());
    }

    private List<?> consoleSelect(Object key) {
        List<?> list = testHelper.evaluate(TestUtils.toLuaSelect("TEST", key));
        return list == null ? Collections.emptyList() : (List<?>) list.get(0);
    }

}
