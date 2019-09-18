package org.tarantool.jdbc;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.tarantool.TestAssumptions.assumeMinimalServerVersion;

import org.tarantool.ServerVersion;
import org.tarantool.TarantoolClientConfig;
import org.tarantool.TarantoolOperation;
import org.tarantool.TarantoolTestHelper;
import org.tarantool.protocol.TarantoolPacket;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.SQLTimeoutException;
import java.sql.Statement;
import java.util.Properties;

public class JdbcConnectionTimeoutIT {

    private static final int LONG_ENOUGH_TIMEOUT = 3000;

    private static TarantoolTestHelper testHelper;

    private Connection connection;

    @BeforeAll
    static void setUpEnv() {
        testHelper = new TarantoolTestHelper("jdbc-connection-timeout-it");
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
        connection = new SQLConnection("", new Properties()) {
            @Override
            protected SQLTarantoolClientImpl makeSqlClient(String address, TarantoolClientConfig config) {
                return new SQLTarantoolClientImpl(address, config) {
                    @Override
                    protected void completeSql(TarantoolOperation operation, TarantoolPacket pack) {
                        try {
                            Thread.sleep(LONG_ENOUGH_TIMEOUT);
                        } catch (InterruptedException ignored) {
                        }
                        super.completeSql(operation, pack);
                    }
                };
            }
        };
    }

    @AfterEach
    void tearDown() throws SQLException {
        if (connection != null) {
            connection.close();
        }
    }

    @Test
    void testShortNetworkTimeout() throws SQLException {
        int tooShortTimeout = 500;
        connection.setNetworkTimeout(null, tooShortTimeout);
        Statement statement = connection.createStatement();
        assertThrows(SQLException.class, () -> statement.executeQuery("SELECT 1"));
        assertTrue(connection.isClosed());
        assertTrue(statement.isClosed());
    }

    @Test
    void testQueryTimeoutIsShorterThanNetwork() throws SQLException {
        int networkTimeout = 2;
        int statementTimeout = 1;

        connection.setNetworkTimeout(null, networkTimeout * 1000);
        Statement statement = connection.createStatement();
        statement.setQueryTimeout(statementTimeout);

        // expect the query timeout won
        assertThrows(SQLTimeoutException.class, () -> statement.executeQuery("SELECT 1"));
        assertFalse(connection.isClosed());
        assertFalse(statement.isClosed());
    }

    @Test
    void testNetworkTimeoutIsShorterThanQuery() throws SQLException {
        int networkTimeout = 1;
        int statementTimeout = 2;

        connection.setNetworkTimeout(null, networkTimeout * 1000);
        Statement statement = connection.createStatement();
        statement.setQueryTimeout(statementTimeout);

        // expect the network timeout won
        assertThrows(SQLException.class, () -> statement.executeQuery("SELECT 1"));
        assertTrue(connection.isClosed());
        assertTrue(statement.isClosed());
    }

    @Test
    void testShortStatementTimeout() throws SQLException {
        int tooShortTimeout = 1;
        Statement statement = connection.createStatement();
        statement.setQueryTimeout(tooShortTimeout);
        assertThrows(SQLTimeoutException.class, () -> statement.executeQuery("SELECT 1"));
        assertFalse(connection.isClosed());
        assertFalse(statement.isClosed());
    }

    @Test
    void testShortPreparedStatementTimeout() throws SQLException {
        int tooShortTimeout = 1;
        PreparedStatement statement = connection.prepareStatement("SELECT ?");
        statement.setInt(1, 1);
        statement.setQueryTimeout(tooShortTimeout);
        assertThrows(SQLTimeoutException.class, statement::executeQuery);
        assertFalse(connection.isClosed());
        assertFalse(statement.isClosed());
    }

}
