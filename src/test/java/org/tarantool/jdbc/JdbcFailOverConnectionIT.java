package org.tarantool.jdbc;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.tarantool.TestAssumptions.assumeMinimalServerVersion;
import static org.tarantool.TestUtils.makeDiscoveryFunction;
import static org.tarantool.jdbc.SqlTestUtils.makeDefaulJdbcUrl;

import org.tarantool.ServerVersion;
import org.tarantool.TarantoolTestHelper;
import org.tarantool.TestUtils;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Extends the {@link org.tarantool.ClientReconnectClusterIT} test suite.
 */
@DisplayName("A JDBC clustered connection")
public class JdbcFailOverConnectionIT {

    private static String REPLICATION_CONFIG = TestUtils.makeReplicationString(
        TarantoolTestHelper.USERNAME,
        TarantoolTestHelper.PASSWORD,
        "localhost:3401",
        "localhost:3402"
    );

    private static final String[] BEFORE_SQL = {
        "DROP TABLE IF EXISTS countries",
        "CREATE TABLE countries(id INT PRIMARY KEY, name VARCHAR(100))",
        "INSERT INTO countries VALUES (67, 'Greece')",
        "INSERT INTO countries VALUES (77, 'Iceland')",
    };

    private static TarantoolTestHelper primaryNode;
    private static TarantoolTestHelper secondaryNode;

    private Connection connection;

    @BeforeAll
    public static void setupEnv() {
        primaryNode = new TarantoolTestHelper("sql-replica1-it");
        secondaryNode = new TarantoolTestHelper("sql-replica2-it");
        primaryNode.createInstance(
            TarantoolTestHelper.LUA_FILE,
            3401,
            3501,
            REPLICATION_CONFIG,
            0.1
        );
        secondaryNode.createInstance(
            TarantoolTestHelper.LUA_FILE,
            3402,
            3502,
            REPLICATION_CONFIG,
            0.1
        );
    }

    @BeforeEach
    public void setUpTest() {
        primaryNode.startInstanceAsync();
        secondaryNode.startInstanceAsync();
        primaryNode.awaitStart();
        secondaryNode.awaitStart();

        primaryNode.executeSql(BEFORE_SQL);
    }

    @AfterEach
    public void tearDownTest() throws SQLException {
        if (connection != null) {
            connection.close();
        }
        primaryNode.stopInstance();
        secondaryNode.stopInstance();
    }

    @Test
    public void testQueryFailOver() throws SQLException {
        assumeMinimalServerVersion(primaryNode.getInstanceVersion(), ServerVersion.V_2_1);
        connection = DriverManager.getConnection(
            makeDefaulJdbcUrl("localhost:3401,localhost:3402", Collections.emptyMap())
        );
        assertFalse(connection.isClosed());

        checkSynchronized(connection);
        primaryNode.stopInstance();
        checkSynchronized(connection);
        secondaryNode.stopInstance();
        assertTrue(connection.isClosed());
    }

    @Test
    public void testRefreshNodes() throws SQLException {
        assumeMinimalServerVersion(primaryNode.getInstanceVersion(), ServerVersion.V_2_1);
        primaryNode.executeLua(
            makeDiscoveryFunction("fetchNodes", Arrays.asList("localhost:3401", "localhost:3402"))
        );

        Map<String, String> parameters = new HashMap<>();
        parameters.put(SQLProperty.CLUSTER_DISCOVERY_ENTRY_FUNCTION.getName(), "fetchNodes");
        connection = DriverManager.getConnection(makeDefaulJdbcUrl("localhost:3401", parameters));
        assertFalse(connection.isClosed());

        checkSynchronized(connection);
        primaryNode.stopInstance();
        checkSynchronized(connection);
        secondaryNode.stopInstance();
        assertTrue(connection.isClosed());
    }

    private void checkSynchronized(Connection connection) throws SQLException {
        try (
            Statement statement = connection.createStatement();
            ResultSet resultSet = statement.executeQuery("SELECT * FROM countries");
        ) {
            resultSet.next();
            assertEquals("Greece", resultSet.getString("name"));
            resultSet.next();
            assertEquals("Iceland", resultSet.getString("name"));
        }
    }
}

