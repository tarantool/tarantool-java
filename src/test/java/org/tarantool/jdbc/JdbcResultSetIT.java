package org.tarantool.jdbc;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.tarantool.TestAssumptions.assumeMinimalServerVersion;

import org.tarantool.ServerVersion;
import org.tarantool.TarantoolTestHelper;
import org.tarantool.util.SQLStates;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;

public class JdbcResultSetIT {

    private static final String[] INIT_SQL = new String[] {
        "CREATE TABLE test(id INT PRIMARY KEY, val VARCHAR(100))",
        "CREATE TABLE test_nulls(id INT PRIMARY KEY, val VARCHAR(100), dig INTEGER, bin SCALAR)",
    };

    private static final String[] CLEAN_SQL = new String[] {
        "DROP TABLE IF EXISTS test",
        "DROP TABLE IF EXISTS test_nulls",
    };

    private static TarantoolTestHelper testHelper;
    private static Connection connection;

    private Statement stmt;

    @BeforeAll
    public static void setupEnv() throws SQLException {
        testHelper = new TarantoolTestHelper("jdbc-resultset-it");
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

        stmt = connection.createStatement();
    }

    @AfterEach
    public void tearDownTest() throws SQLException {
        assumeMinimalServerVersion(testHelper.getInstanceVersion(), ServerVersion.V_2_1);

        testHelper.executeSql(CLEAN_SQL);
        if (stmt != null) {
            stmt.close();
        }
    }

    @Test
    public void testEmpty() throws SQLException {
        ResultSet rs = stmt.executeQuery("SELECT * FROM test WHERE id < 0");
        assertNotNull(rs);
        assertFalse(rs.next());
        rs.close();
    }

    @Test
    public void testIteration() throws SQLException {
        testHelper.executeSql("INSERT INTO test VALUES (1, 'one'), (2, 'two'), (3, 'three')");

        try (ResultSet resultSet = stmt.executeQuery("SELECT * FROM test WHERE id IN (1,2,3) ORDER BY id")) {
            assertNotNull(resultSet);
            assertTrue(resultSet.next());
            assertEquals(1, resultSet.getInt(1));
            assertTrue(resultSet.next());
            assertEquals(2, resultSet.getInt(1));
            assertTrue(resultSet.next());
            assertEquals(3, resultSet.getInt(1));
            assertFalse(resultSet.next());
        }
    }

    @Test
    public void testDefaultScrollType() throws SQLException {
        ResultSet resultSet = stmt.executeQuery("SELECT * FROM test WHERE id < 0");
        assertNotNull(resultSet);
        assertEquals(stmt.getResultSetType(), resultSet.getType());

        stmt.close();
        assertThrows(SQLException.class, resultSet::getType);
    }

    @Test
    public void testSelectedScrollType() throws SQLException {
        Statement statement = connection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
        ResultSet resultSet = statement.executeQuery("SELECT * FROM test WHERE id < 0");
        assertNotNull(resultSet);
        assertEquals(statement.getResultSetType(), resultSet.getType());

        statement.close();
        assertThrows(SQLException.class, resultSet::getType);
    }

    @Test
    public void testOwnedResultSet() throws SQLException {
        ResultSet resultSet = stmt.executeQuery("SELECT * FROM test WHERE id < 0");
        assertNotNull(resultSet);
        assertEquals(stmt, resultSet.getStatement());

        stmt.close();
        assertThrows(SQLException.class, resultSet::getStatement);
    }

    @Test
    public void testResultSetMetadataAfterClose() throws SQLException {
        ResultSet resultSet = stmt.executeQuery("SELECT * FROM test WHERE id < 0");
        assertNotNull(resultSet);
        ResultSetMetaData metaData = resultSet.getMetaData();
        assertNotNull(metaData);

        int expectedColumnSize = 2;
        assertEquals(expectedColumnSize, metaData.getColumnCount());

        resultSet.close();
        assertEquals(expectedColumnSize, metaData.getColumnCount());
    }

    @Test
    public void testHoldability() throws SQLException {
        ResultSet resultSet = stmt.executeQuery("SELECT * FROM test WHERE id < 0");
        assertNotNull(resultSet);
        assertEquals(connection.getMetaData().getResultSetHoldability(), resultSet.getHoldability());
    }

    @Test
    public void testUnwrap() throws SQLException {
        ResultSet resultSet = stmt.executeQuery("SELECT * FROM test WHERE id < 0");
        assertEquals(resultSet, resultSet.unwrap(SQLResultSet.class));
        assertThrows(SQLException.class, () -> resultSet.unwrap(Integer.class));
    }

    @Test
    public void testIsWrapperFor() throws SQLException {
        ResultSet resultSet = stmt.executeQuery("SELECT * FROM test WHERE id < 0");
        assertTrue(resultSet.isWrapperFor(SQLResultSet.class));
        assertFalse(resultSet.isWrapperFor(Integer.class));
    }

    @Test
    public void testNullsSortingAsc() throws SQLException {
        testHelper.executeSql("INSERT INTO test_nulls(id, val) VALUES (1, 'a'), (2, 'b');");
        testHelper.executeSql("INSERT INTO test_nulls(id, val) VALUES (3, 'c'), (4, NULL)");
        testHelper.executeSql("INSERT INTO test_nulls(id, val) VALUES (5, NULL), (6, NULL)");

        ResultSet resultSet = stmt.executeQuery("SELECT * FROM test_nulls ORDER BY val ASC");
        for (int i = 0; i < 3; i++) {
            assertTrue(resultSet.next());
            assertNull(resultSet.getString(2));
        }
        for (int i = 0; i < 3; i++) {
            assertTrue(resultSet.next());
            assertNotNull(resultSet.getString(2));
        }
        assertFalse(resultSet.next());
    }

    @Test
    public void testNullsSortingDesc() throws SQLException {
        testHelper.executeSql("INSERT INTO test_nulls(id, dig) VALUES (1, 1), (2, 2);");
        testHelper.executeSql("INSERT INTO test_nulls(id, dig) VALUES (3, 3), (4, NULL)");
        testHelper.executeSql("INSERT INTO test_nulls(id, dig) VALUES (5, NULL), (6, NULL)");

        ResultSet resultSet = stmt.executeQuery("SELECT id, dig FROM test_nulls ORDER BY val DESC");
        for (int i = 0; i < 3; i++) {
            assertTrue(resultSet.next());
            assertNotNull(resultSet.getString(2));
        }
        for (int i = 0; i < 3; i++) {
            assertTrue(resultSet.next());
            assertNull(resultSet.getString(2));
        }
        assertFalse(resultSet.next());
    }

    @Test
    void testObjectWasNullColumn() throws SQLException {
        testHelper.executeSql("INSERT INTO test_nulls(id, val) VALUES (1, 'z'), (2, 'y');");
        testHelper.executeSql("INSERT INTO test_nulls(id, val) VALUES (3, 'x'), (4, NULL)");

        ResultSet resultSet = stmt.executeQuery("SELECT id, dig FROM test_nulls WHERE val IS NULL");
        resultSet.next();

        resultSet.getInt(1);
        assertFalse(resultSet.wasNull());
        assertNull(resultSet.getString(2));
        assertTrue(resultSet.wasNull());
    }

    @Test
    void testBinaryWasNullColumn() throws SQLException {
        testHelper.executeSql("INSERT INTO test_nulls(id, bin) VALUES (1, 'zz'), (2, 'yy');");
        testHelper.executeSql("INSERT INTO test_nulls(id, bin) VALUES (3, 'xx'), (4, NULL)");

        ResultSet resultSet = stmt.executeQuery("SELECT id, bin FROM test_nulls WHERE bin IS NULL");
        resultSet.next();

        resultSet.getInt(1);
        assertFalse(resultSet.wasNull());
        assertNull(resultSet.getString(2));
        assertTrue(resultSet.wasNull());
        assertNull(resultSet.getAsciiStream(2));
        assertTrue(resultSet.wasNull());
        assertNull(resultSet.getBinaryStream(2));
        assertTrue(resultSet.wasNull());
        assertNull(resultSet.getUnicodeStream(2));
        assertTrue(resultSet.wasNull());
        assertNull(resultSet.getCharacterStream(2));
        assertTrue(resultSet.wasNull());
    }

    @Test
    void testNumberWasNullColumn() throws SQLException {
        testHelper.executeSql("INSERT INTO test_nulls(id, dig) VALUES (1, 1), (2, 2);");
        testHelper.executeSql("INSERT INTO test_nulls(id, dig) VALUES (3, 3), (4, NULL)");

        ResultSet resultSet = stmt.executeQuery("SELECT id, dig FROM test_nulls WHERE dig IS NULL");
        resultSet.next();

        resultSet.getInt(1);
        assertFalse(resultSet.wasNull());
        assertEquals(0, resultSet.getInt(2));
        assertTrue(resultSet.wasNull());
        assertEquals(0, resultSet.getShort(2));
        assertTrue(resultSet.wasNull());
        assertEquals(0, resultSet.getByte(2));
        assertTrue(resultSet.wasNull());
        assertEquals(0, resultSet.getLong(2));
        assertTrue(resultSet.wasNull());
        assertEquals(0, resultSet.getDouble(2));
        assertTrue(resultSet.wasNull());
        assertEquals(0, resultSet.getFloat(2));
        assertTrue(resultSet.wasNull());
    }

    @Test
    public void testFindUniqueColumnLabels() throws SQLException {
        ResultSet resultSet = stmt.executeQuery("SELECT id as f1, val as f2 FROM test");
        assertNotNull(resultSet);
        assertEquals(1, resultSet.findColumn("f1"));
        assertEquals(2, resultSet.findColumn("f2"));
    }

    @Test
    public void testFindDuplicatedColumnLabels() throws SQLException {
        ResultSet resultSet = stmt.executeQuery("SELECT id as f1, val as f1 FROM test");
        assertNotNull(resultSet);
        assertEquals(1, resultSet.findColumn("f1"));
    }

    @Test
    public void testMaxRows() throws SQLException {
        testHelper.executeSql("INSERT INTO test VALUES (1, 'one'), (2, 'two'), (3, 'three')");

        stmt.setMaxRows(1);
        ResultSet resultSet = stmt.executeQuery("SELECT id as f1, val as f2 FROM test");
        assertNotNull(resultSet);
        assertTrue(resultSet.next());
        assertTrue(resultSet.getInt("f1") > 0);
        assertFalse(resultSet.next());
    }

    @Test
    public void testForwardTraversal() throws SQLException {
        testHelper.executeSql("INSERT INTO test VALUES (1, 'one'), (2, 'two'), (3, 'three')");

        ResultSet resultSet = stmt.executeQuery("SELECT id as f1, val as f2 FROM test");
        assertNotNull(resultSet);
        assertTrue(resultSet.isBeforeFirst());
        assertEquals(0, resultSet.getRow());

        assertTrue(resultSet.next());
        assertTrue(resultSet.isFirst());
        assertEquals(1, resultSet.getRow());

        assertTrue(resultSet.next());
        assertEquals(2, resultSet.getRow());

        assertTrue(resultSet.next());
        assertEquals(3, resultSet.getRow());
        assertTrue(resultSet.isLast());

        assertFalse(resultSet.next());
        assertEquals(0, resultSet.getRow());
        assertTrue(resultSet.isAfterLast());

        stmt.close();
        assertThrows(SQLException.class, resultSet::isAfterLast);
    }

    @Test
    public void testTraversal() throws SQLException {
        testHelper.executeSql("INSERT INTO test VALUES (1, 'one'), (2, 'two'), (3, 'three')");

        Statement statement = connection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
        ResultSet resultSet = statement.executeQuery("SELECT id as f1, val as f2 FROM test");
        assertNotNull(resultSet);
        assertTrue(resultSet.isBeforeFirst());
        assertEquals(0, resultSet.getRow());

        assertTrue(resultSet.last());
        assertEquals(3, resultSet.getRow());
        assertTrue(resultSet.isLast());

        assertTrue(resultSet.first());
        assertEquals(1, resultSet.getRow());
        assertTrue(resultSet.isFirst());

        assertFalse(resultSet.relative(-1));
        assertEquals(0, resultSet.getRow());
        assertTrue(resultSet.isBeforeFirst());

        assertTrue(resultSet.relative(1));
        assertEquals(1, resultSet.getRow());
        assertTrue(resultSet.isFirst());

        assertTrue(resultSet.absolute(-1));
        assertEquals(3, resultSet.getRow());
        assertTrue(resultSet.isLast());

        assertTrue(resultSet.absolute(1));
        assertEquals(1, resultSet.getRow());
        assertTrue(resultSet.isFirst());

        resultSet.beforeFirst();
        assertEquals(0, resultSet.getRow());
        assertTrue(resultSet.isBeforeFirst());

        resultSet.afterLast();
        assertEquals(0, resultSet.getRow());
        assertTrue(resultSet.isAfterLast());

        assertTrue(resultSet.previous());
        assertEquals(3, resultSet.getRow());
        assertTrue(resultSet.isLast());

        assertTrue(resultSet.first());
        assertEquals(1, resultSet.getRow());

        assertFalse(resultSet.previous());
        assertEquals(0, resultSet.getRow());
    }

    @Test
    public void testMaxFieldSize() throws SQLException {
        assertEquals(0, stmt.getMaxFieldSize());

        int expectedMaxSize = 256;
        stmt.setMaxFieldSize(expectedMaxSize);
        assertEquals(expectedMaxSize, stmt.getMaxFieldSize());
    }

    @Test
    public void testNegativeMaxFieldSize() throws SQLException {
        SQLException error = assertThrows(SQLException.class, () -> stmt.setMaxFieldSize(-12));
        assertEquals(SQLStates.INVALID_PARAMETER_VALUE.getSqlState(), error.getSQLState());
    }

    @Test
    public void testPositiveMaxFieldSize() throws SQLException {
        testHelper.executeSql("INSERT INTO test VALUES (1, 'greater-than-ten-characters-value')");

        stmt.setMaxFieldSize(10);
        try (ResultSet resultSet = stmt.executeQuery("SELECT * FROM test WHERE id = 1")) {
            resultSet.next();
            assertEquals("greater-th", resultSet.getString(2));
        }
    }

    @Test
    public void testMaxFieldSizeBiggerThanValue() throws SQLException {
        testHelper.executeSql("INSERT INTO test VALUES (1, 'less-than-one-hundred-characters-value')");

        stmt.setMaxFieldSize(100);
        try (ResultSet resultSet = stmt.executeQuery("SELECT * FROM test WHERE id = 1")) {
            resultSet.next();
            assertEquals("less-than-one-hundred-characters-value", resultSet.getString(2));
        }
    }

    @Test
    public void testMaxFieldSizeBinaryType() throws SQLException {
        testHelper.executeSql("CREATE TABLE test_bin(id INT PRIMARY KEY, val SCALAR)");
        testHelper.executeSql("INSERT INTO test_bin VALUES (1, X'6c6f6e672d62696e6172792d737472696e67')");

        stmt.setMaxFieldSize(12);
        try (ResultSet resultSet = stmt.executeQuery("SELECT * FROM test_bin WHERE id = 1")) {
            resultSet.next();
            assertEquals(12, resultSet.getBytes(2).length);
        }

        testHelper.executeSql("DROP TABLE test_bin");
    }

    @Test
    public void testMaxFieldSizeNotTrimmableType() throws SQLException {
        testHelper.executeSql("CREATE TABLE test_num(id INT PRIMARY KEY, val INT)");
        testHelper.executeSql("INSERT INTO test_num VALUES (1, 1234567890)");

        String expectedUntrimmedValue = "1234567890";
        stmt.setMaxFieldSize(5);
        try (ResultSet resultSet = stmt.executeQuery("SELECT * FROM test_num WHERE id = 1")) {
            resultSet.next();
            assertEquals(expectedUntrimmedValue, resultSet.getString(2));
        }

        testHelper.executeSql("DROP TABLE test_num");
    }

}
