package org.tarantool.jdbc;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.tarantool.TestAssumptions.assumeMinimalServerVersion;
import static org.tarantool.jdbc.SqlAssertions.assertSqlExceptionHasStatus;

import org.tarantool.ServerVersion;
import org.tarantool.TarantoolTestHelper;
import org.tarantool.TestUtils;
import org.tarantool.util.SQLStates;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.sql.BatchUpdateException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Statement;
import java.util.Collections;
import java.util.List;

public class JdbcStatementIT {

    private static final String[] INIT_SQL = new String[] {
        "CREATE TABLE test(id INT PRIMARY KEY, val VARCHAR(100))"
    };

    private static final String[] CLEAN_SQL = new String[] {
        "DROP TABLE IF EXISTS test"
    };

    private static TarantoolTestHelper testHelper;
    private static Connection conn;

    private Statement stmt;

    @BeforeAll
    public static void setUpEnv() throws SQLException {
        testHelper = new TarantoolTestHelper("jdbc-statement-it");
        testHelper.createInstance();
        testHelper.startInstance();

        conn = DriverManager.getConnection(SqlTestUtils.makeDefaultJdbcUrl());
    }

    @AfterAll
    public static void tearDownEnv() throws SQLException {
        if (conn != null) {
            conn.close();
        }
        testHelper.stopInstance();
    }

    @BeforeEach
    public void setUpTest() throws SQLException {
        assumeMinimalServerVersion(testHelper.getInstanceVersion(), ServerVersion.V_2_1);
        testHelper.executeSql(INIT_SQL);

        stmt = conn.createStatement();
    }

    @AfterEach
    public void tearDownTest() throws SQLException {
        assumeMinimalServerVersion(testHelper.getInstanceVersion(), ServerVersion.V_2_1);
        testHelper.executeSql(CLEAN_SQL);

        if (stmt != null) {
            stmt.close();
        }
    }

    @Test
    public void testExecuteQuery() throws SQLException {
        testHelper.executeSql("INSERT INTO test(id, val) VALUES (1, 'one')");

        try (ResultSet resultSet = stmt.executeQuery("SELECT val FROM test WHERE id = 1")) {
            assertNotNull(resultSet);
            assertTrue(resultSet.next());
            assertEquals("one", resultSet.getString(1));
            assertFalse(resultSet.next());
        }
    }

    @Test
    public void testExecuteWrongQuery() throws SQLException {
        String wrongResultQuery = "INSERT INTO test(id, val) VALUES (40, 'forty')";

        SQLException exception = assertThrows(SQLException.class, () -> stmt.executeQuery(wrongResultQuery));
        assertSqlExceptionHasStatus(exception, SQLStates.NO_DATA);
    }

    @Test
    public void testExecuteUpdate() throws Exception {
        assertEquals(2, stmt.executeUpdate("INSERT INTO test(id, val) VALUES (10, 'ten'), (20, 'twenty')"));
        assertEquals("ten", consoleSelect(10).get(1));
        assertEquals("twenty", consoleSelect(20).get(1));
    }

    @Test
    public void testExecuteWrongUpdate() throws SQLException {
        String wrongUpdateQuery = "SELECT val FROM test";

        SQLException exception = assertThrows(SQLException.class, () -> stmt.executeUpdate(wrongUpdateQuery));
        assertSqlExceptionHasStatus(exception, SQLStates.TOO_MANY_RESULTS);
    }

    @Test
    public void testExecuteReturnsResultSet() throws SQLException {
        testHelper.executeSql("INSERT INTO test(id, val) VALUES (1, 'one')");

        assertTrue(stmt.execute("SELECT val FROM test WHERE id = 1"));
        try (ResultSet resultSet = stmt.getResultSet()) {
            assertNotNull(resultSet);
            assertTrue(resultSet.next());
            assertEquals("one", resultSet.getString(1));
            assertFalse(resultSet.next());
        }
    }

    @Test
    public void testExecuteReturnsUpdateCount() throws Exception {
        assertFalse(stmt.execute("INSERT INTO test(id, val) VALUES (100, 'hundred'), (1000, 'thousand')"));
        assertEquals(2, stmt.getUpdateCount());

        assertEquals("hundred", consoleSelect(100).get(1));
        assertEquals("thousand", consoleSelect(1000).get(1));
    }

    @Test
    void testGetMaxRows() throws SQLException {
        int defaultMaxSize = 0;
        assertEquals(defaultMaxSize, stmt.getMaxRows());
    }

    @Test
    void testSetMaxRows() throws SQLException {
        int expectedMaxSize = 10;
        stmt.setMaxRows(expectedMaxSize);
        assertEquals(expectedMaxSize, stmt.getMaxRows());
    }

    @Test
    void testSetNegativeMaxRows() {
        int negativeMaxSize = -20;
        assertThrows(SQLException.class, () -> stmt.setMaxRows(negativeMaxSize));
    }

    @Test
    void testGetQueryTimeout() throws SQLException {
        int defaultQueryTimeout = 0;
        assertEquals(defaultQueryTimeout, stmt.getQueryTimeout());
    }

    @Test
    void testSetQueryTimeout() throws SQLException {
        int expectedSeconds = 10;
        stmt.setQueryTimeout(expectedSeconds);
        assertEquals(expectedSeconds, stmt.getQueryTimeout());
    }

    @Test
    void testSetNegativeQueryTimeout() throws SQLException {
        int negativeSeconds = -30;
        assertThrows(SQLException.class, () -> stmt.setQueryTimeout(negativeSeconds));
    }

    @Test
    public void testUnwrap() throws SQLException {
        assertEquals(stmt, stmt.unwrap(TarantoolStatement.class));
        assertEquals(stmt, stmt.unwrap(SQLStatement.class));
        assertThrows(SQLException.class, () -> stmt.unwrap(Integer.class));
    }

    @Test
    public void testIsWrapperFor() throws SQLException {
        assertTrue(stmt.isWrapperFor(TarantoolStatement.class));
        assertTrue(stmt.isWrapperFor(SQLStatement.class));
        assertFalse(stmt.isWrapperFor(Integer.class));
    }

    @Test
    public void testExecuteUpdateNoGeneratedKeys() throws SQLException {
        int affectedRows = stmt.executeUpdate(
            "INSERT INTO test(id, val) VALUES (50, 'fifty')",
            Statement.NO_GENERATED_KEYS
        );
        assertEquals(1, affectedRows);
        ResultSet generatedKeys = stmt.getGeneratedKeys();
        assertNotNull(generatedKeys);
        assertEquals(ResultSet.TYPE_FORWARD_ONLY, generatedKeys.getType());
        assertEquals(ResultSet.CONCUR_READ_ONLY, generatedKeys.getConcurrency());
    }

    @Test
    public void testExecuteNoGeneratedKeys() throws SQLException {
        boolean isResultSet = stmt.execute(
            "INSERT INTO test(id, val) VALUES (60, 'sixty')",
            Statement.NO_GENERATED_KEYS
        );
        assertFalse(isResultSet);
        ResultSet generatedKeys = stmt.getGeneratedKeys();
        assertNotNull(generatedKeys);
        assertFalse(generatedKeys.next());
        assertEquals(ResultSet.TYPE_FORWARD_ONLY, generatedKeys.getType());
        assertEquals(ResultSet.CONCUR_READ_ONLY, generatedKeys.getConcurrency());
    }

    @Test
    void testExecuteUpdateGeneratedKeys() {
        assertThrows(
            SQLException.class,
            () -> stmt.executeUpdate(
                "INSERT INTO test(id, val) VALUES (100, 'hundred'), (1000, 'thousand')",
                Statement.RETURN_GENERATED_KEYS
            )
        );
    }

    @Test
    void testExecuteGeneratedKeys() {
        assertThrows(
            SQLException.class,
            () -> stmt.execute(
                "INSERT INTO test(id, val) VALUES (100, 'hundred'), (1000, 'thousand')",
                Statement.RETURN_GENERATED_KEYS
            )
        );
    }

    @Test
    void testExecuteUpdateWrongGeneratedKeys() {
        int[] wrongConstants = { Integer.MAX_VALUE, Integer.MIN_VALUE, -31, 344 };
        for (int wrongConstant : wrongConstants) {
            assertThrows(SQLException.class,
                () -> stmt.executeUpdate(
                    "INSERT INTO test(id, val) VALUES (100, 'hundred'), (1000, 'thousand')",
                    wrongConstant
                )
            );
        }
    }

    @Test
    void testExecuteWrongGeneratedKeys() {
        int[] wrongConstants = { Integer.MAX_VALUE, Integer.MIN_VALUE, -52, 864 };
        for (int wrongConstant : wrongConstants) {
            assertThrows(SQLException.class,
                () -> stmt.execute(
                    "INSERT INTO test(id, val) VALUES (100, 'hundred'), (1000, 'thousand')",
                    wrongConstant
                )
            );
        }
    }

    @Test
    void testStatementConnection() throws SQLException {
        Statement statement = conn.createStatement();
        assertEquals(conn, statement.getConnection());
    }

    @Test
    void testCloseOnCompletion() throws SQLException {
        assertFalse(stmt.isCloseOnCompletion());
        stmt.closeOnCompletion();
        assertTrue(stmt.isCloseOnCompletion());
    }

    @Test
    void testCloseOnCompletionDisabled() throws SQLException {
        ResultSet resultSet = stmt.executeQuery("SELECT val FROM test WHERE id=1");
        assertFalse(stmt.isClosed());
        assertFalse(resultSet.isClosed());

        resultSet.close();
        assertTrue(resultSet.isClosed());
        assertFalse(stmt.isClosed());
    }

    @Test
    void testCloseOnCompletionEnabled() throws SQLException {
        stmt.closeOnCompletion();
        ResultSet resultSet = stmt.executeQuery("SELECT val FROM test WHERE id=1");

        assertFalse(stmt.isClosed());
        assertFalse(resultSet.isClosed());

        resultSet.close();
        assertTrue(resultSet.isClosed());
        assertTrue(stmt.isClosed());
    }

    @Test
    void testCloseOnCompletionAfterResultSet() throws SQLException {
        ResultSet resultSet = stmt.executeQuery("SELECT val FROM test WHERE id=1");
        stmt.closeOnCompletion();

        assertFalse(stmt.isClosed());
        assertFalse(resultSet.isClosed());

        resultSet.close();
        assertTrue(resultSet.isClosed());
        assertTrue(stmt.isClosed());
    }

    @Test
    void testCloseOnCompletionMultipleResultSets() throws SQLException {
        stmt.closeOnCompletion();
        ResultSet resultSet = stmt.executeQuery("SELECT val FROM test WHERE id=1");
        ResultSet anotherResultSet = stmt.executeQuery("SELECT val FROM test WHERE id=2");

        assertTrue(resultSet.isClosed());
        assertFalse(anotherResultSet.isClosed());
        assertFalse(stmt.isClosed());

        anotherResultSet.close();
        assertTrue(anotherResultSet.isClosed());
        assertTrue(stmt.isClosed());
    }

    @Test
    void testCloseOnCompletionUpdateQueries() throws SQLException {
        stmt.closeOnCompletion();

        int updateCount = stmt.executeUpdate("INSERT INTO test(id, val) VALUES (5, 'five')");
        assertEquals(1, updateCount);
        assertFalse(stmt.isClosed());

        updateCount = stmt.executeUpdate("INSERT INTO test(id, val) VALUES (6, 'six')");
        assertEquals(1, updateCount);
        assertFalse(stmt.isClosed());
    }

    @Test
    void testCloseOnCompletionMixedQueries() throws SQLException {
        stmt.closeOnCompletion();

        int updateCount = stmt.executeUpdate("INSERT INTO test(id, val) VALUES (7, 'seven')");
        assertEquals(1, updateCount);
        assertFalse(stmt.isClosed());

        ResultSet resultSet = stmt.executeQuery("SELECT val FROM test WHERE id=7");
        assertFalse(resultSet.isClosed());
        assertFalse(stmt.isClosed());

        updateCount = stmt.executeUpdate("INSERT INTO test(id, val) VALUES (8, 'eight')");
        assertEquals(1, updateCount);
        assertTrue(resultSet.isClosed());
        assertFalse(stmt.isClosed());

        resultSet = stmt.executeQuery("SELECT val FROM test WHERE id=8");
        assertFalse(resultSet.isClosed());
        assertFalse(stmt.isClosed());

        resultSet.close();
        assertTrue(resultSet.isClosed());
        assertTrue(stmt.isClosed());
    }

    @Test
    public void testMoreResultsWithResultSet() throws SQLException {
        stmt.execute("SELECT val FROM test WHERE id = 1");

        ResultSet rs = stmt.getResultSet();

        assertFalse(rs.isClosed());
        assertFalse(stmt.getMoreResults());
        assertEquals(-1, stmt.getUpdateCount());
        assertTrue(rs.isClosed());
    }

    @Test
    public void testMoreResultsWithUpdateCount() throws SQLException {
        stmt.execute("INSERT INTO test(id, val) VALUES (9, 'nine')");

        assertEquals(1, stmt.getUpdateCount());
        assertFalse(stmt.getMoreResults());
        assertEquals(-1, stmt.getUpdateCount());
    }

    @Test
    public void testMoreResultsButCloseCurrent() throws SQLException {
        stmt.execute("SELECT val FROM test WHERE id = 1");

        ResultSet resultSet = stmt.getResultSet();

        assertFalse(resultSet.isClosed());
        assertFalse(stmt.getMoreResults(Statement.CLOSE_CURRENT_RESULT));
        assertEquals(-1, stmt.getUpdateCount());
        assertTrue(resultSet.isClosed());
    }

    @Test
    public void testMoreResultsButCloseAll() throws SQLException {
        stmt.execute("SELECT val FROM test WHERE id = 3");
        assertThrows(SQLFeatureNotSupportedException.class, () -> stmt.getMoreResults(Statement.CLOSE_ALL_RESULTS));

        stmt.execute("INSERT INTO test(id, val) VALUES (21, 'twenty one')");
        assertEquals(1, stmt.getUpdateCount());
        assertFalse(stmt.getMoreResults(Statement.CLOSE_ALL_RESULTS));
        assertEquals(-1, stmt.getUpdateCount());
    }

    @Test
    public void testMoreResultsButKeepCurrent() throws SQLException {
        stmt.execute("SELECT val FROM test WHERE id = 2");
        assertThrows(SQLFeatureNotSupportedException.class, () -> stmt.getMoreResults(Statement.KEEP_CURRENT_RESULT));

        stmt.execute("INSERT INTO test(id, val) VALUES (22, 'twenty two')");
        assertEquals(1, stmt.getUpdateCount());
        assertFalse(stmt.getMoreResults(Statement.KEEP_CURRENT_RESULT));
        assertEquals(-1, stmt.getUpdateCount());
    }

    @Test
    public void testExecuteOneBatchQuery() throws Exception {
        stmt.addBatch("INSERT INTO test(id, val) VALUES (1, 'one')");
        int[] updateCounts = stmt.executeBatch();
        assertEquals(1, updateCounts.length);
        assertEquals(1, updateCounts[0]);

        assertEquals("one", consoleSelect(1).get(1));
    }

    @Test
    public void testExecuteZeroBatchQuery() throws Exception {
        int[] updateCounts = stmt.executeBatch();
        assertEquals(0, updateCounts.length);
    }

    @Test
    public void testExecuteBatchQuery() throws Exception {
        stmt.addBatch("INSERT INTO test(id, val) VALUES (1, 'one')");
        stmt.addBatch("INSERT INTO test(id, val) VALUES (2, 'two')");
        stmt.addBatch("INSERT INTO test(id, val) VALUES (3, 'three'), (4, 'four')");
        stmt.addBatch("DELETE FROM test WHERE id > 1");

        int[] updateCounts = stmt.executeBatch();
        assertEquals(4, updateCounts.length);
        assertEquals(1, updateCounts[0]);
        assertEquals(1, updateCounts[1]);
        assertEquals(2, updateCounts[2]);
        assertEquals(3, updateCounts[3]);

        assertEquals("one", consoleSelect(1).get(1));
    }

    @Test
    public void testClearBatch() throws Exception {
        stmt.addBatch("INSERT INTO test(id, val) VALUES (1, 'one')");
        stmt.addBatch("INSERT INTO test(id, val) VALUES (2, 'two')");
        stmt.clearBatch();
        int[] updateCounts = stmt.executeBatch();
        assertEquals(0, updateCounts.length);
    }

    @Test
    public void testExecuteZeroCountsBatchQuery() throws Exception {
        stmt.addBatch("INSERT INTO test(id, val) VALUES (50, 'fifty')");
        stmt.addBatch("DELETE FROM test WHERE id > 100");
        int[] updateCounts = stmt.executeBatch();
        assertEquals(2, updateCounts.length);
        assertEquals(1, updateCounts[0]);
        assertEquals(0, updateCounts[1]);

        assertEquals("fifty", consoleSelect(50).get(1));
    }

    @Test
    public void testExecuteMixedBatchQuery() throws Exception {
        stmt.addBatch("INSERT INTO test(id, val) VALUES (5, 'five')");
        stmt.addBatch("DELETE FROM test WHERE id = 5");
        stmt.addBatch("INSERT INTO test(id, val) VALUES (5, 'five')");
        stmt.addBatch("INSERT INTO test(id, val) VALUES (6, 'six')");

        int[] updateCounts = stmt.executeBatch();
        assertEquals(4, updateCounts.length);
        assertEquals(1, updateCounts[0]);
        assertEquals(1, updateCounts[1]);
        assertEquals(1, updateCounts[2]);
        assertEquals(1, updateCounts[3]);
    }

    @Test
    public void testExecuteFailedBatchQuery() throws Exception {
        stmt.addBatch("INSERT INTO test(id, val) VALUES (5, 'five')");
        stmt.addBatch("INSERT INTO test(id, val) VALUES (5, 'five')");
        stmt.addBatch("INSERT INTO test(id, val) VALUES (6, 'six')");

        BatchUpdateException exception = assertThrows(BatchUpdateException.class, () -> stmt.executeBatch());
        int[] updateCounts = exception.getUpdateCounts();
        assertEquals(3, updateCounts.length);
        assertEquals(1, updateCounts[0]);
        assertEquals(Statement.EXECUTE_FAILED, updateCounts[1]);
        assertEquals(1, updateCounts[2]);

        assertEquals("five", consoleSelect(5).get(1));
        assertEquals("six", consoleSelect(6).get(1));
    }

    @Test
    public void testExecuteResultSetBatchQuery() throws Exception {
        stmt.addBatch("INSERT INTO test(id, val) VALUES (5, 'five')");
        stmt.addBatch("SELECT * FROM test WHERE id = 5");
        stmt.addBatch("INSERT INTO test(id, val) VALUES (6, 'six')");
        stmt.addBatch("SELECT id FROM test WHERE id = 8");

        BatchUpdateException exception = assertThrows(BatchUpdateException.class, () -> stmt.executeBatch());
        int[] updateCounts = exception.getUpdateCounts();
        assertEquals(4, updateCounts.length);
        assertEquals(1, updateCounts[0]);
        assertEquals(Statement.EXECUTE_FAILED, updateCounts[1]);
        assertEquals(1, updateCounts[2]);
        assertEquals(Statement.EXECUTE_FAILED, updateCounts[3]);

        assertEquals("five", consoleSelect(5).get(1));
        assertEquals("six", consoleSelect(6).get(1));
    }

    @Test
    void testPoolableStatus() throws SQLException {
        assertFalse(stmt.isPoolable());
        stmt.setPoolable(true);
        assertTrue(stmt.isPoolable());
    }

    private List<?> consoleSelect(Object key) {
        List<?> list = testHelper.evaluate(TestUtils.toLuaSelect("TEST", key));
        return list == null ? Collections.emptyList() : (List<?>) list.get(0);
    }

}
