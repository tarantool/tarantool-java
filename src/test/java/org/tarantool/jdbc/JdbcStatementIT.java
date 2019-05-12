package org.tarantool.jdbc;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import org.tarantool.util.SQLStates;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class JdbcStatementIT extends AbstractJdbcIT {

    private Statement stmt;

    @BeforeEach
    public void setUp() throws SQLException {
        stmt = conn.createStatement();
    }

    @AfterEach
    public void tearDown() throws SQLException {
        if (stmt != null && !stmt.isClosed()) {
            stmt.close();
        }
    }

    @Test
    public void testExecuteQuery() throws SQLException {
        ResultSet rs = stmt.executeQuery("SELECT val FROM test WHERE id=1");
        assertNotNull(rs);
        assertTrue(rs.next());
        assertEquals("one", rs.getString(1));
        assertFalse(rs.next());
        rs.close();
    }

    @Test
    public void testExecuteWrongQuery() throws SQLException {
        String wrongResultQuery = "INSERT INTO test(id, val) VALUES (40, 'forty')";

        SQLException exception = assertThrows(SQLException.class, () -> stmt.executeQuery(wrongResultQuery));
        SqlAssertions.assertSqlExceptionHasStatus(exception, SQLStates.NO_DATA);
    }

    @Test
    public void testExecuteUpdate() throws Exception {
        assertEquals(2, stmt.executeUpdate("INSERT INTO test(id, val) VALUES (10, 'ten'), (20, 'twenty')"));
        assertEquals("ten", getRow("test", 10).get(1));
        assertEquals("twenty", getRow("test", 20).get(1));
    }

    @Test
    public void testExecuteWrongUpdate() throws SQLException {
        String wrongUpdateQuery = "SELECT val FROM test";

        SQLException exception = assertThrows(SQLException.class, () -> stmt.executeUpdate(wrongUpdateQuery));
        SqlAssertions.assertSqlExceptionHasStatus(exception, SQLStates.TOO_MANY_RESULTS);
    }

    @Test
    public void testExecuteReturnsResultSet() throws SQLException {
        assertTrue(stmt.execute("SELECT val FROM test WHERE id=1"));
        ResultSet rs = stmt.getResultSet();
        assertNotNull(rs);
        assertTrue(rs.next());
        assertEquals("one", rs.getString(1));
        assertFalse(rs.next());
        rs.close();
    }

    @Test
    public void testExecuteReturnsUpdateCount() throws Exception {
        assertFalse(stmt.execute("INSERT INTO test(id, val) VALUES (100, 'hundred'), (1000, 'thousand')"));
        assertEquals(2, stmt.getUpdateCount());

        assertEquals("hundred", getRow("test", 100).get(1));
        assertEquals("thousand", getRow("test", 1000).get(1));
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
    public void testClosedConnection() throws Exception {
        conn.close();

        int i = 0;
        for (; i < 3; i++) {
            final int step = i;
            SQLException e = assertThrows(SQLException.class, new Executable() {
                @Override
                public void execute() throws Throwable {
                    switch (step) {
                    case 0:
                        stmt.executeQuery("TEST");
                        break;
                    case 1:
                        stmt.executeUpdate("TEST");
                        break;
                    case 2:
                        stmt.execute("TEST");
                        break;
                    default:
                        fail();
                    }
                }
            });
            assertEquals("Statement is closed.", e.getMessage());
        }
        assertEquals(3, i);
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

}
