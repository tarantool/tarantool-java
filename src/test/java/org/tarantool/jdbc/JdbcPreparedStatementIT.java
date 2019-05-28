package org.tarantool.jdbc;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.tarantool.TestAssumptions.assumeMinimalServerVersion;

import org.tarantool.ServerVersion;
import org.tarantool.TarantoolTestHelper;
import org.tarantool.TestUtils;
import org.tarantool.util.SQLStates;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collections;
import java.util.List;

public class JdbcPreparedStatementIT {

    private static final String[] INIT_SQL = new String[] {
        "CREATE TABLE test(id INT PRIMARY KEY, val VARCHAR(100))",
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
        prep = conn.prepareStatement("INSERT INTO test VALUES (?, ?)");
        prep.setInt(1, 200);
        prep.setString(2, "two hundred");

        SQLException exception = assertThrows(SQLException.class, () -> prep.executeQuery());
        SqlAssertions.assertSqlExceptionHasStatus(exception, SQLStates.NO_DATA);
    }

    @Test
    public void testExecuteUpdate() throws Exception {
        prep = conn.prepareStatement("INSERT INTO test VALUES(?, ?)");
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
        prep = conn.prepareStatement("INSERT INTO test VALUES(?, ?), (?, ?)");
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
        prep = conn.prepareStatement("INSERT INTO test values (50, 'fifty')", Statement.NO_GENERATED_KEYS);
        assertFalse(prep.execute());
        assertEquals(1, prep.getUpdateCount());

        ResultSet generatedKeys = prep.getGeneratedKeys();
        assertNotNull(generatedKeys);
        assertEquals(ResultSet.TYPE_FORWARD_ONLY, generatedKeys.getType());
        assertEquals(ResultSet.CONCUR_READ_ONLY, generatedKeys.getConcurrency());
    }

    @Test
    void testStatementConnection() throws SQLException {
        Statement statement = conn.prepareStatement("SELECT * FROM TEST");
        assertEquals(conn, statement.getConnection());
    }

    private List<?> consoleSelect(Object key) {
        List<?> list = testHelper.evaluate(TestUtils.toLuaSelect("TEST", key));
        return list == null ? Collections.emptyList() : (List<?>) list.get(0);
    }

}
