package org.tarantool.jdbc;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.tarantool.TestUtils.makeInstanceEnv;

import org.tarantool.TarantoolClientConfig;
import org.tarantool.TarantoolControl;
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

    protected static final String LUA_FILE = "jdk-testing.lua";
    protected static final int LISTEN = 3301;
    protected static final int ADMIN = 3313;
    private static final String INSTANCE_NAME = "jdk-testing";
    private static final int LONG_ENOUGH_TIMEOUT = 3000;

    private Connection connection;

    @BeforeAll
    public static void setUpEnv() {
        TarantoolControl control = new TarantoolControl();
        control.createInstance(INSTANCE_NAME, LUA_FILE, makeInstanceEnv(LISTEN, ADMIN));
        control.start(INSTANCE_NAME);
    }

    @AfterAll
    public static void tearDownEnv() {
        TarantoolControl control = new TarantoolControl();
        control.stop(INSTANCE_NAME);
    }

    @BeforeEach
    void setUp() throws SQLException {
        connection = new SQLConnection("", new Properties()) {
            @Override
            protected SQLTarantoolClientImpl makeSqlClient(String address, TarantoolClientConfig config) {
                return new SQLTarantoolClientImpl(address, config) {
                    @Override
                    protected void completeSql(TarantoolOp<?> operation, TarantoolPacket pack) {
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
        connection.close();
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
