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
import java.sql.Types;

@DisplayName("A resultSet metadata")
public class JdbcResultSetMetaDataIT {

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
    }

    @AfterEach
    public void tearDownTest() throws SQLException {
        assumeMinimalServerVersion(testHelper.getInstanceVersion(), ServerVersion.V_2_1);
        testHelper.executeSql(CLEAN_SQL);
    }

    @Test
    @DisplayName("returned correct column names")
    public void testColumnNames() throws SQLException {
        testHelper.executeSql("CREATE TABLE test(id INT PRIMARY KEY, val VARCHAR(100))");
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
        testHelper.executeSql("CREATE TABLE test(id INT PRIMARY KEY)");
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
        testHelper.executeSql("CREATE TABLE test(id INT PRIMARY KEY)");
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
        testHelper.executeSql("CREATE TABLE test(id INT PRIMARY KEY, val TEXT)");
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
        testHelper.executeSql("CREATE TABLE test(id INT PRIMARY KEY, num_val INT)");
        try (Statement statement = connection.createStatement()) {
            assertNotNull(statement);

            try (ResultSet resultSet = statement.executeQuery("SELECT id AS alias_id FROM test")) {
                assertNotNull(resultSet);
                ResultSetMetaData metaData = resultSet.getMetaData();
                assertEquals("ALIAS_ID", metaData.getColumnLabel(1).toUpperCase());
            }
            try (ResultSet resultSet = statement.executeQuery("SELECT num_val AS alias_val FROM test")) {
                assertNotNull(resultSet);
                ResultSetMetaData metaData = resultSet.getMetaData();
                assertEquals("ALIAS_VAL", metaData.getColumnLabel(1).toUpperCase());
            }
            try (ResultSet resultSet = statement.executeQuery("SELECT * FROM test")) {
                assertNotNull(resultSet);
                ResultSetMetaData metaData = resultSet.getMetaData();
                assertEquals("ID", metaData.getColumnLabel(1).toUpperCase());
                assertEquals("NUM_VAL", metaData.getColumnLabel(2).toUpperCase());
            }
        }
    }

    @Test
    @DisplayName("returned an error when column index is out of range")
    public void testWrongColumnAliases() throws SQLException {
        testHelper.executeSql("CREATE TABLE test(id INT PRIMARY KEY, val TEXT, num_val INT)");
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

    @Test
    @DisplayName("returned case sensitive columns")
    public void testCaseSensitiveColumns() throws SQLException {
        testHelper.executeSql(
            "CREATE TABLE test(id INT PRIMARY KEY, char_val VARCHAR(100), text_val TEXT, bin_val SCALAR)"
        );
        try (
            Statement statement = connection.createStatement();
            ResultSet resultSet = statement.executeQuery("SELECT char_val, text_val, bin_val FROM test")
        ) {
            ResultSetMetaData rsMeta = resultSet.getMetaData();

            assertTrue(rsMeta.isCaseSensitive(1));
            assertTrue(rsMeta.isCaseSensitive(2));
            assertTrue(rsMeta.isCaseSensitive(3));
        }
    }

    @Test
    @DisplayName("returned case insensitive columns")
    public void testCaseInsensitiveColumns() throws SQLException {
        testHelper.executeSql(
            "CREATE TABLE test(id INT PRIMARY KEY, num_val DOUBLE)"
        );
        try (
            Statement statement = connection.createStatement();
            ResultSet resultSet = statement.executeQuery("SELECT * FROM test")
        ) {
            ResultSetMetaData rsMeta = resultSet.getMetaData();

            assertFalse(rsMeta.isCaseSensitive(1));
            assertFalse(rsMeta.isCaseSensitive(2));
        }
    }

    @Test
    @DisplayName("returned searchable columns")
    public void testSearchableColumns() throws SQLException {
        testHelper.executeSql(
            "CREATE TABLE test(id INT PRIMARY KEY, num_val DOUBLE, text_val TEXT, bin_val SCALAR)"
        );
        try (
            Statement statement = connection.createStatement();
            ResultSet resultSet = statement.executeQuery("SELECT * FROM test")
        ) {
            ResultSetMetaData rsMeta = resultSet.getMetaData();

            assertTrue(rsMeta.isSearchable(1));
            assertTrue(rsMeta.isSearchable(2));
            assertTrue(rsMeta.isSearchable(3));
            assertTrue(rsMeta.isSearchable(4));
        }
    }

    @Test
    @DisplayName("returned no monetary columns")
    public void testCurrencyColumns() throws SQLException {
        testHelper.executeSql(
            "CREATE TABLE test(id INT PRIMARY KEY, num_val DOUBLE, text_val TEXT, bin_val SCALAR)"
        );
        try (
            Statement statement = connection.createStatement();
            ResultSet resultSet = statement.executeQuery("SELECT * FROM test")
        ) {
            ResultSetMetaData rsMeta = resultSet.getMetaData();

            assertFalse(rsMeta.isCurrency(1));
            assertFalse(rsMeta.isCurrency(2));
            assertFalse(rsMeta.isCurrency(3));
            assertFalse(rsMeta.isCurrency(4));
        }
    }

    @Test
    @DisplayName("returned signed columns")
    public void testSignedColumns() throws SQLException {
        testHelper.executeSql(
            "CREATE TABLE test(id INT PRIMARY KEY, double_val DOUBLE, real_val REAL, float_val FLOAT)"
        );
        try (
            Statement statement = connection.createStatement();
            ResultSet resultSet = statement.executeQuery("SELECT * FROM test")
        ) {
            ResultSetMetaData rsMeta = resultSet.getMetaData();

            assertTrue(rsMeta.isSigned(1));
            assertTrue(rsMeta.isSigned(2));
            assertTrue(rsMeta.isSigned(3));
            assertTrue(rsMeta.isSigned(4));
        }
    }

    @Test
    @DisplayName("returned not signed columns")
    public void testNotSignedColumns() throws SQLException {
        testHelper.executeSql(
            "CREATE TABLE test(id INT PRIMARY KEY, char_val VARCHAR(100), text_val TEXT, bin_val SCALAR)"
        );
        try (
            Statement statement = connection.createStatement();
            ResultSet resultSet = statement.executeQuery("SELECT * FROM test")
        ) {
            ResultSetMetaData rsMeta = resultSet.getMetaData();

            assertTrue(rsMeta.isSigned(1));
            assertFalse(rsMeta.isSigned(2));
            assertFalse(rsMeta.isSigned(3));
            assertFalse(rsMeta.isSigned(4));
        }
    }

    @Test
    @DisplayName("returned numeric column types")
    public void testColumnsNumericTypes() throws SQLException {
        testHelper.executeSql(
            "CREATE TABLE test(id INT PRIMARY KEY, f_val FLOAT, d_val DOUBLE, r_val REAL)"
        );
        try (
            Statement statement = connection.createStatement();
            ResultSet resultSet = statement.executeQuery("SELECT * FROM test")
        ) {
            ResultSetMetaData rsMeta = resultSet.getMetaData();

            assertEquals(Types.INTEGER, rsMeta.getColumnType(1));
            assertEquals("integer", rsMeta.getColumnTypeName(1));
            assertEquals("java.lang.Integer", rsMeta.getColumnClassName(1));

            // we cannot distinguish numeric types because Tarantool
            // receives double noSQL type for all the numeric SQL types
            assertEquals(Types.DOUBLE, rsMeta.getColumnType(2));
            assertEquals("double", rsMeta.getColumnTypeName(2));
            assertEquals("java.lang.Double", rsMeta.getColumnClassName(2));

            assertEquals(Types.DOUBLE, rsMeta.getColumnType(3));
            assertEquals("double", rsMeta.getColumnTypeName(3));
            assertEquals("java.lang.Double", rsMeta.getColumnClassName(3));

            assertEquals(Types.DOUBLE, rsMeta.getColumnType(4));
            assertEquals("double", rsMeta.getColumnTypeName(4));
            assertEquals("java.lang.Double", rsMeta.getColumnClassName(4));
        }
    }

    @Test
    @DisplayName("returned textual column types")
    public void testColumnsTextualTypes() throws SQLException {
        testHelper.executeSql(
            "CREATE TABLE test(id INT PRIMARY KEY, v_val VARCHAR(10), t_val TEXT, b_val SCALAR)"
        );
        try (
            Statement statement = connection.createStatement();
            ResultSet resultSet = statement.executeQuery("SELECT * FROM test")
        ) {
            ResultSetMetaData rsMeta = resultSet.getMetaData();

            assertEquals(Types.INTEGER, rsMeta.getColumnType(1));
            assertEquals("integer", rsMeta.getColumnTypeName(1));
            assertEquals("java.lang.Integer", rsMeta.getColumnClassName(1));

            assertEquals(Types.VARCHAR, rsMeta.getColumnType(2));
            assertEquals("varchar", rsMeta.getColumnTypeName(2));
            assertEquals("java.lang.String", rsMeta.getColumnClassName(2));

            // TEXT and VARCHAR are not distinguishable
            assertEquals(Types.VARCHAR, rsMeta.getColumnType(3));
            assertEquals("varchar", rsMeta.getColumnTypeName(3));
            assertEquals("java.lang.String", rsMeta.getColumnClassName(3));

            assertEquals(Types.BINARY, rsMeta.getColumnType(4));
            assertEquals("scalar", rsMeta.getColumnTypeName(4));
            assertEquals("[B", rsMeta.getColumnClassName(4));
        }
    }

}
