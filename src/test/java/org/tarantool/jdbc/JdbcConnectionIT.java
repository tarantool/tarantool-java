package org.tarantool.jdbc;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.tarantool.TestAssumptions.assumeMinimalServerVersion;

import org.tarantool.ServerVersion;
import org.tarantool.TarantoolTestHelper;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Statement;

public class JdbcConnectionIT {

    private static TarantoolTestHelper testHelper;

    private Connection conn;

    @BeforeAll
    public static void setupEnv() {
        testHelper = new TarantoolTestHelper("jdbc-connection-it");
        testHelper.createInstance();
        testHelper.startInstance();
    }

    @AfterAll
    public static void teardownEnv() {
        testHelper.stopInstance();
    }

    @BeforeEach
    public void setUpTest() throws SQLException {
        assumeMinimalServerVersion(testHelper.getInstanceVersion(), ServerVersion.V_2_1);
        conn = DriverManager.getConnection(SqlTestUtils.makeDefaultJdbcUrl());
    }

    @AfterEach
    public void tearDownTest() throws SQLException {
        assumeMinimalServerVersion(testHelper.getInstanceVersion(), ServerVersion.V_2_1);
        if (conn != null) {
            conn.close();
        }
    }

    @Test
    public void testCreateStatement() throws SQLException {
        Statement stmt = conn.createStatement();
        assertNotNull(stmt);
        stmt.close();
    }

    @Test
    public void testPrepareStatement() throws SQLException {
        PreparedStatement prep = conn.prepareStatement("INSERT INTO test(id, val) VALUES(?, ?)");
        assertNotNull(prep);
        prep.close();
    }

    @Test
    public void testCloseIsClosed() throws SQLException {
        assertFalse(conn.isClosed());
        conn.close();
        assertTrue(conn.isClosed());
        conn.close();
    }

    @Test
    public void testGetMetaData() throws SQLException {
        DatabaseMetaData meta = conn.getMetaData();
        assertNotNull(meta);
    }

    @Test
    public void testGetSetNetworkTimeout() throws Exception {
        assertEquals(0, conn.getNetworkTimeout());
        SQLException e = assertThrows(SQLException.class, new Executable() {
            @Override
            public void execute() throws Throwable {
                conn.setNetworkTimeout(null, -1);
            }
        });
        assertEquals("Network timeout cannot be negative.", e.getMessage());
        conn.setNetworkTimeout(null, 3000);
        assertEquals(3000, conn.getNetworkTimeout());
    }

    @Test
    void testIsValidCheck() throws SQLException {
        assertTrue(conn.isValid(2));
        assertThrows(SQLException.class, () -> conn.isValid(-1));

        conn.close();
        assertFalse(conn.isValid(2));
    }

    @Test
    public void testConnectionUnwrap() throws SQLException {
        assertEquals(conn, conn.unwrap(SQLConnection.class));
        assertThrows(SQLException.class, () -> conn.unwrap(Integer.class));
    }

    @Test
    public void testConnectionIsWrapperFor() throws SQLException {
        assertTrue(conn.isWrapperFor(SQLConnection.class));
        assertFalse(conn.isWrapperFor(Integer.class));
    }

    @Test
    public void testDefaultGetHoldability() throws SQLException {
        // default connection holdability should be equal to metadata one
        assertEquals(conn.getMetaData().getResultSetHoldability(), conn.getHoldability());
    }

    @Test
    public void testSetAndGetHoldability() throws SQLException {
        conn.setHoldability(ResultSet.HOLD_CURSORS_OVER_COMMIT);
        assertEquals(ResultSet.HOLD_CURSORS_OVER_COMMIT, conn.getHoldability());

        assertThrows(
            SQLFeatureNotSupportedException.class,
            () -> conn.setHoldability(ResultSet.CLOSE_CURSORS_AT_COMMIT)
        );
        assertThrows(SQLException.class, () -> conn.setHoldability(Integer.MAX_VALUE));
    }

    @Test
    public void testCreateHoldableStatement() throws SQLException {
        Statement statement = conn.createStatement();
        assertEquals(ResultSet.HOLD_CURSORS_OVER_COMMIT, statement.getResultSetHoldability());

        statement = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
        assertEquals(ResultSet.HOLD_CURSORS_OVER_COMMIT, statement.getResultSetHoldability());

        statement = conn.createStatement(
            ResultSet.TYPE_FORWARD_ONLY,
            ResultSet.CONCUR_READ_ONLY,
            ResultSet.HOLD_CURSORS_OVER_COMMIT
        );
        assertEquals(ResultSet.HOLD_CURSORS_OVER_COMMIT, statement.getResultSetHoldability());
    }

    @Test
    public void testCreateUnsupportedHoldableStatement() throws SQLException {
        assertThrows(
            SQLFeatureNotSupportedException.class,
            () -> conn.createStatement(
                ResultSet.TYPE_FORWARD_ONLY,
                ResultSet.CONCUR_READ_ONLY,
                ResultSet.CLOSE_CURSORS_AT_COMMIT
            ));
    }

    @Test
    public void testCreateWrongHoldableStatement() throws SQLException {
        assertThrows(SQLException.class, () -> {
            conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY, Integer.MAX_VALUE);
        });
        assertThrows(SQLException.class, () -> {
            conn.createStatement(
                ResultSet.TYPE_FORWARD_ONLY,
                ResultSet.CONCUR_READ_ONLY,
                -65
            );
        });
    }

    @Test
    public void testPrepareHoldableStatement() throws SQLException {
        String sqlString = "TEST";
        Statement statement = conn.prepareStatement(sqlString);
        assertEquals(ResultSet.HOLD_CURSORS_OVER_COMMIT, statement.getResultSetHoldability());

        statement = conn.prepareStatement(sqlString, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
        assertEquals(ResultSet.HOLD_CURSORS_OVER_COMMIT, statement.getResultSetHoldability());

        statement = conn.prepareStatement(
            sqlString,
            ResultSet.TYPE_FORWARD_ONLY,
            ResultSet.CONCUR_READ_ONLY,
            ResultSet.HOLD_CURSORS_OVER_COMMIT
        );
        assertEquals(ResultSet.HOLD_CURSORS_OVER_COMMIT, statement.getResultSetHoldability());
    }

    @Test
    public void testPrepareUnsupportedHoldableStatement() throws SQLException {
        assertThrows(SQLFeatureNotSupportedException.class,
            () -> {
                String sqlString = "SELECT * FROM TEST";
                conn.prepareStatement(
                    sqlString,
                    ResultSet.TYPE_FORWARD_ONLY,
                    ResultSet.CONCUR_READ_ONLY,
                    ResultSet.CLOSE_CURSORS_AT_COMMIT
                );
            });
    }

    @Test
    public void testPrepareWrongHoldableStatement() throws SQLException {
        String sqlString = "SELECT * FROM TEST";
        assertThrows(SQLException.class,
            () -> {
                conn.prepareStatement(
                    sqlString,
                    ResultSet.TYPE_FORWARD_ONLY,
                    ResultSet.CONCUR_READ_ONLY,
                    Integer.MAX_VALUE
                );
            });
        assertThrows(SQLException.class,
            () -> {
                conn.prepareStatement(
                    sqlString,
                    ResultSet.TYPE_FORWARD_ONLY,
                    ResultSet.CONCUR_READ_ONLY, -190
                );
            });
    }

    @Test
    public void testCreateScrollableStatement() throws SQLException {
        Statement statement = conn.createStatement();
        assertEquals(ResultSet.TYPE_FORWARD_ONLY, statement.getResultSetType());

        statement = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
        assertEquals(ResultSet.TYPE_FORWARD_ONLY, statement.getResultSetType());

        statement = conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
        assertEquals(ResultSet.TYPE_SCROLL_INSENSITIVE, statement.getResultSetType());

        statement = conn.createStatement(
            ResultSet.TYPE_FORWARD_ONLY,
            ResultSet.CONCUR_READ_ONLY,
            ResultSet.HOLD_CURSORS_OVER_COMMIT
        );
        assertEquals(ResultSet.TYPE_FORWARD_ONLY, statement.getResultSetType());

        statement = conn.createStatement(
            ResultSet.TYPE_SCROLL_INSENSITIVE,
            ResultSet.CONCUR_READ_ONLY,
            ResultSet.HOLD_CURSORS_OVER_COMMIT
        );
        assertEquals(ResultSet.TYPE_SCROLL_INSENSITIVE, statement.getResultSetType());
    }

    @Test
    public void testCreateUnsupportedScrollableStatement() throws SQLException {
        assertThrows(SQLFeatureNotSupportedException.class, () -> {
            conn.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_READ_ONLY);
        });
        assertThrows(SQLFeatureNotSupportedException.class, () -> {
            conn.createStatement(
                ResultSet.TYPE_SCROLL_SENSITIVE,
                ResultSet.CONCUR_READ_ONLY,
                ResultSet.HOLD_CURSORS_OVER_COMMIT
            );
        });
    }

    @Test
    public void testCreateWrongScrollableStatement() {
        assertThrows(SQLException.class, () -> {
            conn.createStatement(Integer.MAX_VALUE, ResultSet.CONCUR_READ_ONLY, ResultSet.HOLD_CURSORS_OVER_COMMIT);
        });
        assertThrows(SQLException.class, () -> {
            conn.createStatement(-47, ResultSet.CONCUR_READ_ONLY, ResultSet.HOLD_CURSORS_OVER_COMMIT);
        });
    }

    @Test
    public void testPrepareScrollableStatement() throws SQLException {
        String sqlString = "TEST";
        Statement statement = conn.prepareStatement(sqlString);
        assertEquals(ResultSet.TYPE_FORWARD_ONLY, statement.getResultSetType());

        statement = conn.prepareStatement(sqlString, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
        assertEquals(ResultSet.TYPE_FORWARD_ONLY, statement.getResultSetType());

        statement = conn.prepareStatement(
            sqlString,
            ResultSet.TYPE_FORWARD_ONLY,
            ResultSet.CONCUR_READ_ONLY,
            ResultSet.HOLD_CURSORS_OVER_COMMIT
        );
        assertEquals(ResultSet.TYPE_FORWARD_ONLY, statement.getResultSetType());
    }

    @Test
    public void testPrepareUnsupportedScrollableStatement() throws SQLException {
        assertThrows(SQLFeatureNotSupportedException.class, () -> {
            String sqlString = "SELECT * FROM TEST";
            conn.prepareStatement(sqlString, ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_READ_ONLY);
        });
        assertThrows(SQLFeatureNotSupportedException.class, () -> {
            String sqlString = "SELECT * FROM TEST";
            conn.prepareStatement(
                sqlString,
                ResultSet.TYPE_SCROLL_SENSITIVE,
                ResultSet.CONCUR_READ_ONLY,
                ResultSet.CLOSE_CURSORS_AT_COMMIT
            );
        });
    }

    @Test
    public void testPrepareWrongScrollableStatement() throws SQLException {
        String sqlString = "SELECT * FROM TEST";
        assertThrows(SQLException.class,
            () -> {
                conn.prepareStatement(
                    sqlString,
                    ResultSet.TYPE_FORWARD_ONLY,
                    ResultSet.CONCUR_READ_ONLY,
                    Integer.MAX_VALUE
                );
            });
        assertThrows(SQLException.class, () -> {
            conn.prepareStatement(sqlString, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY, -90);
        });
    }

    @Test
    public void testCreateConcurrentStatement() throws SQLException {
        Statement statement = conn.createStatement();
        assertEquals(ResultSet.CONCUR_READ_ONLY, statement.getResultSetConcurrency());

        statement = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
        assertEquals(ResultSet.CONCUR_READ_ONLY, statement.getResultSetConcurrency());

        statement = conn.createStatement(
            ResultSet.TYPE_FORWARD_ONLY,
            ResultSet.CONCUR_READ_ONLY,
            ResultSet.HOLD_CURSORS_OVER_COMMIT
        );
        assertEquals(ResultSet.CONCUR_READ_ONLY, statement.getResultSetConcurrency());
    }

    @Test
    public void testCreateUnsupportedConcurrentStatement() throws SQLException {
        assertThrows(SQLFeatureNotSupportedException.class, () -> {
            conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE);
        });
        assertThrows(SQLFeatureNotSupportedException.class,
            () -> {
                conn.createStatement(
                    ResultSet.TYPE_FORWARD_ONLY,
                    ResultSet.CONCUR_UPDATABLE,
                    ResultSet.HOLD_CURSORS_OVER_COMMIT
                );
            });
    }

    @Test
    public void testCreateWrongConcurrentStatement() {
        assertThrows(SQLException.class, () -> {
            conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, Integer.MAX_VALUE, ResultSet.HOLD_CURSORS_OVER_COMMIT);
        });
        assertThrows(SQLException.class, () -> {
            conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, -7213, ResultSet.HOLD_CURSORS_OVER_COMMIT);
        });
    }

    @Test
    public void testCreateStatementWithClosedConnection() {
        assertThrows(SQLException.class,
            () -> {
                conn.close();
                conn.createStatement(
                    ResultSet.TYPE_FORWARD_ONLY,
                    ResultSet.CONCUR_READ_ONLY,
                    ResultSet.HOLD_CURSORS_OVER_COMMIT
                );
            });
        assertThrows(SQLException.class,
            () -> {
                conn.close();
                conn.createStatement(
                    ResultSet.TYPE_SCROLL_INSENSITIVE,
                    ResultSet.CONCUR_READ_ONLY,
                    ResultSet.HOLD_CURSORS_OVER_COMMIT
                );
            });
    }

    @Test
    public void testPrepareStatementWithClosedConnection() {
        String sqlString = "SELECT * FROM TEST";
        assertThrows(SQLException.class,
            () -> {
                conn.close();
                conn.prepareStatement(
                    sqlString,
                    ResultSet.TYPE_FORWARD_ONLY,
                    ResultSet.CONCUR_READ_ONLY,
                    ResultSet.HOLD_CURSORS_OVER_COMMIT
                );
            });
        assertThrows(SQLException.class,
            () -> {
                conn.close();
                conn.prepareStatement(
                    sqlString,
                    ResultSet.TYPE_SCROLL_INSENSITIVE,
                    ResultSet.CONCUR_READ_ONLY,
                    ResultSet.HOLD_CURSORS_OVER_COMMIT
                );
            });
    }

    @Test
    public void testGeneratedKeys() throws SQLException {
        String sql = "SELECT * FROM test";
        PreparedStatement preparedStatement = conn.prepareStatement(sql, Statement.NO_GENERATED_KEYS);
        assertNotNull(preparedStatement);
        preparedStatement.close();

        assertThrows(SQLFeatureNotSupportedException.class, () -> conn.prepareStatement(sql, new int[] { 1 }));
        assertThrows(SQLFeatureNotSupportedException.class, () -> conn.prepareStatement(sql, new String[] { "id" }));

        assertThrows(SQLException.class, () -> conn.prepareStatement(sql, Integer.MAX_VALUE));
        assertThrows(SQLException.class, () -> conn.prepareStatement(sql, Integer.MIN_VALUE));
        assertThrows(SQLException.class, () -> conn.prepareStatement(sql, -76));
        assertThrows(
            SQLFeatureNotSupportedException.class,
            () -> conn.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS)
        );
    }

}
