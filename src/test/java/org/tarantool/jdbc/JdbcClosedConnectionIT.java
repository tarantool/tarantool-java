package org.tarantool.jdbc;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.fail;
import static org.tarantool.TestAssumptions.assumeMinimalServerVersion;
import static org.tarantool.jdbc.SqlAssertions.assertSqlExceptionHasStatus;

import org.tarantool.TarantoolTestHelper;
import org.tarantool.util.SQLStates;
import org.tarantool.util.ServerVersion;

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
import java.sql.SQLClientInfoException;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collections;
import java.util.Properties;

public class JdbcClosedConnectionIT {

    private static final int LONG_ENOUGH_TIMEOUT = 3000;

    private static TarantoolTestHelper testHelper;

    private Connection connection;

    @BeforeAll
    static void setUpEnv() {
        testHelper = new TarantoolTestHelper("jdbc-closed-connection-it");
        testHelper.createInstance();
        testHelper.startInstance();
    }

    @AfterAll
    static void tearDownEnv() {
        testHelper.stopInstance();
    }

    @BeforeEach
    void setUp() throws SQLException {
        assumeMinimalServerVersion(testHelper.getInstanceVersion(), ServerVersion.V_2_1);
        connection = DriverManager.getConnection(SqlTestUtils.makeDefaultJdbcUrl());
    }

    @AfterEach
    void tearDown() throws SQLException {
        if (connection != null) {
            connection.close();
        }
    }

    @Test
    public void testMetadata() throws SQLException {
        DatabaseMetaData metaData = connection.getMetaData();
        connection.close();

        int i = 0;
        for (; i < 3; i++) {
            final int step = i;
            SQLException e = assertThrows(SQLException.class, new Executable() {
                @Override
                public void execute() throws Throwable {
                    switch (step) {
                    case 0:
                        metaData.getTables(null, null, null, new String[] { "TABLE" });
                        break;
                    case 1:
                        metaData.getColumns(null, null, "TEST", null);
                        break;
                    case 2:
                        metaData.getPrimaryKeys(null, null, "TEST");
                        break;
                    default:
                        fail();
                    }
                }
            });
            assertEquals("Connection is closed.", e.getCause().getMessage());
        }
        assertEquals(3, i);
    }

    @Test
    public void testPreparedStatement() throws SQLException {
        PreparedStatement preparedStatement = connection.prepareStatement("SELECT 1");
        connection.close();

        int i = 0;
        for (; i < 3; i++) {
            final int step = i;
            SQLException e = assertThrows(SQLException.class, new Executable() {
                @Override
                public void execute() throws Throwable {
                    switch (step) {
                    case 0:
                        preparedStatement.executeQuery();
                        break;
                    case 1:
                        preparedStatement.executeUpdate();
                        break;
                    case 2:
                        preparedStatement.execute();
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
    public void testStatement() throws Exception {
        Statement statement = connection.createStatement();
        connection.close();

        int i = 0;
        for (; i < 3; i++) {
            final int step = i;
            SQLException e = assertThrows(SQLException.class, new Executable() {
                @Override
                public void execute() throws Throwable {
                    switch (step) {
                    case 0:
                        statement.executeQuery("TEST");
                        break;
                    case 1:
                        statement.executeUpdate("TEST");
                        break;
                    case 2:
                        statement.execute("TEST");
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
    public void testSettingsMethodAfterClose() throws SQLException {
        connection.close();

        SQLException sqlException;
        sqlException = assertThrows(SQLException.class, () -> connection.clearWarnings());
        assertSqlExceptionHasStatus(sqlException, SQLStates.CONNECTION_DOES_NOT_EXIST);

        sqlException = assertThrows(SQLException.class, () -> connection.getWarnings());
        assertSqlExceptionHasStatus(sqlException, SQLStates.CONNECTION_DOES_NOT_EXIST);

        sqlException = assertThrows(SQLException.class, () -> connection.getHoldability());
        assertSqlExceptionHasStatus(sqlException, SQLStates.CONNECTION_DOES_NOT_EXIST);
        sqlException = assertThrows(SQLException.class, () -> connection.getNetworkTimeout());
        assertSqlExceptionHasStatus(sqlException, SQLStates.CONNECTION_DOES_NOT_EXIST);

        sqlException = assertThrows(
            SQLException.class,
            () -> connection.setHoldability(ResultSet.HOLD_CURSORS_OVER_COMMIT)
        );
        assertSqlExceptionHasStatus(sqlException, SQLStates.CONNECTION_DOES_NOT_EXIST);
        sqlException = assertThrows(SQLException.class, () -> connection.setNetworkTimeout(null, 1000));
        assertSqlExceptionHasStatus(sqlException, SQLStates.CONNECTION_DOES_NOT_EXIST);
    }

    @Test
    public void testStatementMethodsAfterClose() throws SQLException {
        connection.close();

        SQLException sqlException;
        sqlException = assertThrows(SQLException.class, () -> connection.createStatement());
        assertSqlExceptionHasStatus(sqlException, SQLStates.CONNECTION_DOES_NOT_EXIST);
        sqlException = assertThrows(
            SQLException.class,
            () -> connection.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)
        );
        assertSqlExceptionHasStatus(sqlException, SQLStates.CONNECTION_DOES_NOT_EXIST);
        sqlException = assertThrows(SQLException.class, () ->
            connection.createStatement(
                ResultSet.TYPE_FORWARD_ONLY,
                ResultSet.CONCUR_READ_ONLY,
                ResultSet.HOLD_CURSORS_OVER_COMMIT
            )
        );
        assertSqlExceptionHasStatus(sqlException, SQLStates.CONNECTION_DOES_NOT_EXIST);
    }

    @Test
    public void testPreparedMethodsAfterClose() throws SQLException {
        connection.close();

        SQLException sqlException;
        sqlException = assertThrows(SQLException.class, () -> connection.prepareStatement(""));
        assertSqlExceptionHasStatus(sqlException, SQLStates.CONNECTION_DOES_NOT_EXIST);
        sqlException = assertThrows(SQLException.class, () ->
            connection.prepareStatement("", ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)
        );
        assertSqlExceptionHasStatus(sqlException, SQLStates.CONNECTION_DOES_NOT_EXIST);
        sqlException = assertThrows(SQLException.class, () ->
            connection.prepareStatement(
                "",
                ResultSet.TYPE_FORWARD_ONLY,
                ResultSet.CONCUR_READ_ONLY,
                ResultSet.HOLD_CURSORS_OVER_COMMIT
            )
        );
        assertSqlExceptionHasStatus(sqlException, SQLStates.CONNECTION_DOES_NOT_EXIST);
        sqlException = assertThrows(
            SQLException.class,
            () -> connection.prepareStatement("", Statement.NO_GENERATED_KEYS)
        );
        assertSqlExceptionHasStatus(sqlException, SQLStates.CONNECTION_DOES_NOT_EXIST);
        sqlException = assertThrows(SQLException.class, () -> connection.prepareStatement("", new int[] { }));
        assertSqlExceptionHasStatus(sqlException, SQLStates.CONNECTION_DOES_NOT_EXIST);
        sqlException = assertThrows(SQLException.class, () -> connection.prepareStatement("", new String[] { }));
        assertSqlExceptionHasStatus(sqlException, SQLStates.CONNECTION_DOES_NOT_EXIST);
    }

    @Test
    public void testCallableMethodsAfterClose() throws SQLException {
        connection.close();

        SQLException sqlException;
        sqlException = assertThrows(SQLException.class, () -> connection.prepareCall(""));
        assertSqlExceptionHasStatus(sqlException, SQLStates.CONNECTION_DOES_NOT_EXIST);
        sqlException = assertThrows(SQLException.class, () ->
            connection.prepareCall("", ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)
        );
        assertSqlExceptionHasStatus(sqlException, SQLStates.CONNECTION_DOES_NOT_EXIST);
        sqlException = assertThrows(SQLException.class, () ->
            connection.prepareCall(
                "",
                ResultSet.TYPE_FORWARD_ONLY,
                ResultSet.CONCUR_READ_ONLY,
                ResultSet.HOLD_CURSORS_OVER_COMMIT
            )
        );
        assertSqlExceptionHasStatus(sqlException, SQLStates.CONNECTION_DOES_NOT_EXIST);
    }

    @Test
    public void testClientInfoMethodsAfterClose() throws SQLException {
        connection.close();

        SQLException sqlException;
        sqlException = assertThrows(SQLClientInfoException.class, () -> connection.setClientInfo(new Properties()));
        assertSqlExceptionHasStatus(sqlException, SQLStates.CONNECTION_DOES_NOT_EXIST);
        sqlException = assertThrows(SQLClientInfoException.class, () -> connection.setClientInfo("key", "value"));
        assertSqlExceptionHasStatus(sqlException, SQLStates.CONNECTION_DOES_NOT_EXIST);
        sqlException = assertThrows(SQLException.class, () -> connection.getClientInfo("param1"));
        assertSqlExceptionHasStatus(sqlException, SQLStates.CONNECTION_DOES_NOT_EXIST);
        sqlException = assertThrows(SQLException.class, () -> connection.getClientInfo());
        assertSqlExceptionHasStatus(sqlException, SQLStates.CONNECTION_DOES_NOT_EXIST);
    }

    @Test
    public void testTransactionMethodsAfterClose() throws SQLException {
        connection.close();

        SQLException sqlException;
        sqlException = assertThrows(SQLException.class, () -> connection.commit());
        assertSqlExceptionHasStatus(sqlException, SQLStates.CONNECTION_DOES_NOT_EXIST);
        sqlException = assertThrows(SQLException.class, () -> connection.rollback());
        assertSqlExceptionHasStatus(sqlException, SQLStates.CONNECTION_DOES_NOT_EXIST);
        sqlException = assertThrows(SQLException.class, () -> connection.rollback(null));
        assertSqlExceptionHasStatus(sqlException, SQLStates.CONNECTION_DOES_NOT_EXIST);
        sqlException = assertThrows(SQLException.class, () -> connection.setAutoCommit(true));
        assertSqlExceptionHasStatus(sqlException, SQLStates.CONNECTION_DOES_NOT_EXIST);
        sqlException = assertThrows(SQLException.class, () -> connection.getAutoCommit());
        assertSqlExceptionHasStatus(sqlException, SQLStates.CONNECTION_DOES_NOT_EXIST);
        sqlException = assertThrows(SQLException.class, () -> connection.setSavepoint());
        assertSqlExceptionHasStatus(sqlException, SQLStates.CONNECTION_DOES_NOT_EXIST);
        sqlException = assertThrows(SQLException.class, () -> connection.setSavepoint(""));
        assertSqlExceptionHasStatus(sqlException, SQLStates.CONNECTION_DOES_NOT_EXIST);
        sqlException = assertThrows(SQLException.class, () -> connection.isReadOnly());
        assertSqlExceptionHasStatus(sqlException, SQLStates.CONNECTION_DOES_NOT_EXIST);
        sqlException = assertThrows(SQLException.class, () -> connection.setReadOnly(true));
        assertSqlExceptionHasStatus(sqlException, SQLStates.CONNECTION_DOES_NOT_EXIST);
        sqlException = assertThrows(SQLException.class, () -> connection.getTransactionIsolation());
        assertSqlExceptionHasStatus(sqlException, SQLStates.CONNECTION_DOES_NOT_EXIST);
        sqlException = assertThrows(
            SQLException.class,
            () -> connection.setTransactionIsolation(Connection.TRANSACTION_NONE)
        );
        assertSqlExceptionHasStatus(sqlException, SQLStates.CONNECTION_DOES_NOT_EXIST);
    }

    @Test
    public void testMetadataMethodsAfterClose() throws SQLException {
        connection.close();

        SQLException sqlException;
        sqlException = assertThrows(SQLException.class, () -> connection.getMetaData());
        assertSqlExceptionHasStatus(sqlException, SQLStates.CONNECTION_DOES_NOT_EXIST);
        sqlException = assertThrows(SQLException.class, () -> connection.nativeSQL(""));
        assertSqlExceptionHasStatus(sqlException, SQLStates.CONNECTION_DOES_NOT_EXIST);

        sqlException = assertThrows(SQLException.class, () -> connection.getSchema());
        assertSqlExceptionHasStatus(sqlException, SQLStates.CONNECTION_DOES_NOT_EXIST);
        sqlException = assertThrows(SQLException.class, () -> connection.getCatalog());
        assertSqlExceptionHasStatus(sqlException, SQLStates.CONNECTION_DOES_NOT_EXIST);
        sqlException = assertThrows(SQLException.class, () -> connection.setCatalog(""));
        assertSqlExceptionHasStatus(sqlException, SQLStates.CONNECTION_DOES_NOT_EXIST);
        sqlException = assertThrows(SQLException.class, () -> connection.setSchema(""));
        assertSqlExceptionHasStatus(sqlException, SQLStates.CONNECTION_DOES_NOT_EXIST);
    }

    @Test
    public void testTypeMethodsAfterClose() throws SQLException {
        connection.close();

        SQLException sqlException;
        sqlException = assertThrows(
            SQLException.class,
            () -> connection.createArrayOf("INTEGER", new Object[] { 1, 2, 3 })
        );
        assertSqlExceptionHasStatus(sqlException, SQLStates.CONNECTION_DOES_NOT_EXIST);
        sqlException = assertThrows(SQLException.class, () -> connection.createBlob());
        assertSqlExceptionHasStatus(sqlException, SQLStates.CONNECTION_DOES_NOT_EXIST);
        sqlException = assertThrows(SQLException.class, () -> connection.createClob());
        assertSqlExceptionHasStatus(sqlException, SQLStates.CONNECTION_DOES_NOT_EXIST);
        sqlException = assertThrows(SQLException.class, () -> connection.createNClob());
        assertSqlExceptionHasStatus(sqlException, SQLStates.CONNECTION_DOES_NOT_EXIST);
        sqlException = assertThrows(SQLException.class, () -> connection.createSQLXML());
        assertSqlExceptionHasStatus(sqlException, SQLStates.CONNECTION_DOES_NOT_EXIST);

        sqlException = assertThrows(SQLException.class, () -> connection.getTypeMap());
        assertSqlExceptionHasStatus(sqlException, SQLStates.CONNECTION_DOES_NOT_EXIST);
        sqlException = assertThrows(SQLException.class, () -> connection.setTypeMap(Collections.emptyMap()));
        assertSqlExceptionHasStatus(sqlException, SQLStates.CONNECTION_DOES_NOT_EXIST);
    }

}
