package org.tarantool.jdbc;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.tarantool.TestAssumptions.assumeMinimalServerVersion;

import org.tarantool.TarantoolTestHelper;
import org.tarantool.util.SQLStates;
import org.tarantool.util.ServerVersion;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.JDBCType;
import java.sql.ParameterMetaData;
import java.sql.PreparedStatement;
import java.sql.SQLException;

@DisplayName("A parameter metadata")
public class JdbcParameterMetaDataIT {

    private static final String[] INIT_SQL = new String[] {
        "CREATE TABLE test(id INT PRIMARY KEY, val VARCHAR(100), bin_val SCALAR)",
    };

    private static final String[] CLEAN_SQL = new String[] {
        "DROP TABLE IF EXISTS test"
    };

    private static TarantoolTestHelper testHelper;
    private static Connection connection;

    @BeforeAll
    public static void setupEnv() throws SQLException {
        testHelper = new TarantoolTestHelper("jdbc-param-metadata-it");
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
        assumeMinimalServerVersion(testHelper.getInstanceVersion(), ServerVersion.V_2_3);
        testHelper.executeSql(INIT_SQL);
    }

    @AfterEach
    public void tearDownTest() throws SQLException {
        assumeMinimalServerVersion(testHelper.getInstanceVersion(), ServerVersion.V_2_3);
        testHelper.executeSql(CLEAN_SQL);
    }

    @Test
    @DisplayName("fetched parameter metadata")
    public void testPreparedParameterMetaData() throws SQLException {
        try (PreparedStatement statement =
                 connection.prepareStatement("SELECT val FROM test WHERE id = ? AND val = ?")) {
            ParameterMetaData parameterMetaData = statement.getParameterMetaData();
            assertNotNull(parameterMetaData);
            assertEquals(2, parameterMetaData.getParameterCount());
            assertEquals(JDBCType.OTHER.getVendorTypeNumber(), parameterMetaData.getParameterType(1));
            assertEquals(ParameterMetaData.parameterModeIn, parameterMetaData.getParameterMode(1));
            assertEquals(ParameterMetaData.parameterNullableUnknown, parameterMetaData.isNullable(1));
        }
    }

    @Test
    @DisplayName("failed to get info by wrong parameter index")
    public void testWrongParameterIndex() throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement("INSERT INTO test VALUES (?, ?, ?)")) {
            ParameterMetaData parameterMetaData = statement.getParameterMetaData();
            assertNotNull(parameterMetaData);
            SQLException biggerThanMaxError = assertThrows(
                SQLException.class,
                () -> parameterMetaData.getParameterMode(4)
            );
            SQLException lessThanZeroError = assertThrows(
                SQLException.class,
                () -> parameterMetaData.getParameterMode(-4)
            );

            assertEquals(biggerThanMaxError.getSQLState(), SQLStates.INVALID_PARAMETER_VALUE.getSqlState());
            assertEquals(lessThanZeroError.getSQLState(), SQLStates.INVALID_PARAMETER_VALUE.getSqlState());
        }
    }

    @Test
    @DisplayName("unwrapped correct")
    public void testUnwrap() throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement("SELECT val FROM test")) {
            ParameterMetaData metaData = statement.getParameterMetaData();
            assertEquals(metaData, metaData.unwrap(SQLParameterMetaData.class));
            assertThrows(SQLException.class, () -> metaData.unwrap(Integer.class));
        }
    }

    @Test
    @DisplayName("checked as a proper wrapper")
    public void testIsWrapperFor() throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement("SELECT * FROM test")) {
            ParameterMetaData metaData = statement.getParameterMetaData();
            assertTrue(metaData.isWrapperFor(SQLParameterMetaData.class));
            assertFalse(statement.isWrapperFor(Integer.class));
        }
    }

}
