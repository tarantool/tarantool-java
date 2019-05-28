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
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;

@DisplayName("A resultSet metadata")
public class JdbcResultSetMetaDataIT {

    private static final String[] INIT_SQL = new String[] {
        "CREATE TABLE test(id INT PRIMARY KEY, val VARCHAR(100))"
    };

    private static final String[] CLEAN_SQL = new String[] {
        "DROP TABLE IF EXISTS test"
    };

    private static TarantoolTestHelper testHelper;
    private static Connection connection;

    @BeforeAll
    public static void setupEnv() throws SQLException {
        testHelper = new TarantoolTestHelper("jdbc-rs-metadata-it");
        testHelper.createInstance();
        testHelper.startInstance();

        connection = DriverManager.getConnection(SqlTestUtils.makeDefaultJdbcUrl());
    }

    @AfterAll
    public static void teardownEnv() throws SQLException {
        if (connection != null) {
            connection.close();
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
    }

    @Test
    @DisplayName("returned correct column names")
    public void testColumnNames() throws SQLException {
        try (
            Statement statement = connection.createStatement();
            ResultSet resultSet = statement.executeQuery("SELECT * FROM test")
        ) {
            ResultSetMetaData rsMeta = resultSet.getMetaData();

            int columnCount = 2;
            assertEquals(columnCount, rsMeta.getColumnCount());
            assertEquals("ID", rsMeta.getColumnName(1));
            assertEquals("VAL", rsMeta.getColumnName(2));
        }
    }

    @Test
    @DisplayName("unwrapped correct")
    public void testUnwrap() throws SQLException {
        try (
            Statement statement = connection.createStatement();
            ResultSet resultSet = statement.executeQuery("SELECT * FROM test")
        ) {
            ResultSetMetaData metaData = resultSet.getMetaData();
            assertEquals(metaData, metaData.unwrap(SQLResultSetMetaData.class));
            assertThrows(SQLException.class, () -> metaData.unwrap(Integer.class));
        }
    }

    @Test
    @DisplayName("checked as a proper wrapper")
    public void testIsWrapperFor() throws SQLException {
        try (
            Statement statement = connection.createStatement();
            ResultSet resultSet = statement.executeQuery("SELECT * FROM test")
        ) {
            ResultSetMetaData metaData = resultSet.getMetaData();
            assertTrue(metaData.isWrapperFor(SQLResultSetMetaData.class));
            assertFalse(metaData.isWrapperFor(Integer.class));
        }
    }

    @Test
    @DisplayName("returned a correct result columns size")
    public void testColumnCount() throws SQLException {
        try (Statement statement = connection.createStatement()) {
            assertNotNull(statement);

            try (ResultSet resultSet = statement.executeQuery("SELECT * FROM test")) {
                assertNotNull(resultSet);
                ResultSetMetaData metaData = resultSet.getMetaData();
                assertEquals(2, metaData.getColumnCount());
            }
            try (ResultSet resultSet = statement.executeQuery("SELECT id, val FROM test")) {
                assertNotNull(resultSet);
                ResultSetMetaData metaData = resultSet.getMetaData();
                assertEquals(2, metaData.getColumnCount());
            }
            try (ResultSet resultSet = statement.executeQuery("SELECT id FROM test")) {
                assertNotNull(resultSet);
                ResultSetMetaData metaData = resultSet.getMetaData();
                assertEquals(1, metaData.getColumnCount());
            }
        }
    }

    @Test
    @DisplayName("returned correct result column aliases")
    public void testColumnAliases() throws SQLException {
        try (Statement statement = connection.createStatement()) {
            assertNotNull(statement);

            try (ResultSet resultSet = statement.executeQuery("SELECT id AS alias_id FROM test")) {
                assertNotNull(resultSet);
                ResultSetMetaData metaData = resultSet.getMetaData();
                assertEquals("ALIAS_ID", metaData.getColumnLabel(1).toUpperCase());
            }
            try (ResultSet resultSet = statement.executeQuery("SELECT val AS alias_val FROM test")) {
                assertNotNull(resultSet);
                ResultSetMetaData metaData = resultSet.getMetaData();
                assertEquals("ALIAS_VAL", metaData.getColumnLabel(1).toUpperCase());
            }
            try (ResultSet resultSet = statement.executeQuery("SELECT * FROM test")) {
                assertNotNull(resultSet);
                ResultSetMetaData metaData = resultSet.getMetaData();
                assertEquals("ID", metaData.getColumnLabel(1).toUpperCase());
                assertEquals("VAL", metaData.getColumnLabel(2).toUpperCase());
            }
        }
    }

    @Test
    @DisplayName("returned an error when column index is out of range")
    public void testWrongColumnAliases() throws SQLException {
        try (Statement statement = connection.createStatement()) {
            assertNotNull(statement);

            try (ResultSet resultSet = statement.executeQuery("SELECT * FROM test")) {
                assertNotNull(resultSet);
                ResultSetMetaData metaData = resultSet.getMetaData();
                int columnsNumber = metaData.getColumnCount();
                assertThrows(SQLException.class, () -> metaData.getColumnLabel(columnsNumber + 1));
                assertThrows(SQLException.class, () -> metaData.getColumnLabel(-5));
                assertThrows(SQLException.class, () -> metaData.getColumnLabel(Integer.MAX_VALUE));
                assertThrows(SQLException.class, () -> metaData.getColumnLabel(Integer.MIN_VALUE));
            }
        }
    }

}
