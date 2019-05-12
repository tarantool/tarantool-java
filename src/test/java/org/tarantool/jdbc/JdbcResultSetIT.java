package org.tarantool.jdbc;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;

public class JdbcResultSetIT extends JdbcTypesIT {

    private Statement stmt;
    private DatabaseMetaData metaData;

    @BeforeEach
    public void setUp() throws Exception {
        stmt = conn.createStatement();
        metaData = conn.getMetaData();
    }

    @AfterEach
    public void tearDown() throws Exception {
        if (stmt != null && !stmt.isClosed()) {
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
        ResultSet rs = stmt.executeQuery("SELECT * FROM test WHERE id IN (1,2,3) ORDER BY id");
        assertNotNull(rs);
        assertTrue(rs.next());
        assertEquals(1, rs.getInt(1));
        assertTrue(rs.next());
        assertEquals(2, rs.getInt(1));
        assertTrue(rs.next());
        assertEquals(3, rs.getInt(1));
        assertFalse(rs.next());
        rs.close();
    }

    @Test
    public void testGetByteColumn() throws SQLException {
        makeHelper(Byte.class)
            .setColumns(TntSqlType.INT, TntSqlType.INTEGER)
            .setValues(BYTE_VALS)
            .testGetColumn();
    }

    @Test
    public void testGetShortColumn() throws SQLException {
        makeHelper(Short.class)
            .setColumns(TntSqlType.INT, TntSqlType.INTEGER)
            .setValues(SHORT_VALS)
            .testGetColumn();
    }

    @Test
    public void testGetIntColumn() throws SQLException {
        makeHelper(Integer.class)
            .setColumns(TntSqlType.INT, TntSqlType.INTEGER)
            .setValues(INT_VALS)
            .testGetColumn();
    }

    @Test
    public void testGetLongColumn() throws SQLException {
        makeHelper(Long.class)
            .setColumns(TntSqlType.INT, TntSqlType.INTEGER)
            .setValues(LONG_VALS)
            .testGetColumn();
    }

    @Test
    public void testGetBigDecimalColumn() throws SQLException {
        makeHelper(BigDecimal.class)
            .setColumns(TntSqlType.REAL, TntSqlType.FLOAT, TntSqlType.DOUBLE)
            .setValues(BIGDEC_VALS)
            .testGetColumn();
    }

    @Test
    public void testGetFloatColumn() throws SQLException {
        makeHelper(Float.class)
            .setColumns(TntSqlType.REAL)
            .setValues(FLOAT_VALS)
            .testGetColumn();
    }

    @Test
    public void testGetDoubleColumn() throws SQLException {
        makeHelper(Double.class)
            .setColumns(TntSqlType.FLOAT, TntSqlType.DOUBLE)
            .setValues(DOUBLE_VALS)
            .testGetColumn();
    }

    @Test
    public void testGetStringColumn() throws SQLException {
        makeHelper(String.class)
            .setColumns(TntSqlType.VARCHAR, TntSqlType.TEXT)
            .setValues(STRING_VALS)
            .testGetColumn();
    }

    @Test
    public void testGetByteArrayColumn() throws SQLException {
        makeHelper(byte[].class)
            .setColumns(TntSqlType.SCALAR)
            .setValues(BINARY_VALS)
            .testGetColumn();
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
        Statement statement = conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
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
        assertEquals(metaData.getResultSetHoldability(), resultSet.getHoldability());
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
        ResultSet resultSet = stmt.executeQuery("SELECT id, dig FROM test_nulls WHERE val IS NULL");
        resultSet.next();

        resultSet.getInt(1);
        assertFalse(resultSet.wasNull());
        assertNull(resultSet.getString(2));
        assertTrue(resultSet.wasNull());
    }

    @Test
    void testBinaryWasNullColumn() throws SQLException {
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
        stmt.setMaxRows(1);
        ResultSet resultSet = stmt.executeQuery("SELECT id as f1, val as f2 FROM test");
        assertNotNull(resultSet);
        assertTrue(resultSet.next());
        assertTrue(resultSet.getInt("f1") > 0);
        assertFalse(resultSet.next());
    }

    @Test
    public void testForwardTraversal() throws SQLException {
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
        Statement statement = conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
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

}
