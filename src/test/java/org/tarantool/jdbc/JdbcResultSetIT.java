package org.tarantool.jdbc;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
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

}
