package org.tarantool.jdbc;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeEach;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class JdbcResultSetIT extends AbstractJdbcIT {
    private Statement stmt;

    @BeforeEach
    public void setUp() throws Exception {
        stmt = conn.createStatement();
    }

    @AfterEach
    public void tearDown() throws Exception {
        if (stmt != null && !stmt.isClosed())
            stmt.close();
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
    public void testGetColumnByIdx() throws SQLException {
        ResultSet rs = stmt.executeQuery("SELECT * FROM test_types");
        assertNotNull(rs);
        assertTrue(rs.next());

        assertEquals(testRow[0], rs.getInt(1));//INT
        assertEquals(testRow[1], rs.getString(2));//CHAR
        assertEquals(testRow[2], rs.getString(3));//VARCHAR
        assertEquals(testRow[3], rs.getString(4)); //LONGVARCHAR
        assertEquals(testRow[4], rs.getBigDecimal(5));// NUMERIC
        assertEquals(testRow[5], rs.getBigDecimal(6));// DECIMAL
        assertEquals(testRow[6], rs.getBoolean(7));//BIT
        assertEquals(testRow[7], rs.getByte(8));//TINYINT
        assertEquals(testRow[8], rs.getShort(9));//SMALLINT
        assertEquals(testRow[9], rs.getInt(10));//INTEGER
        assertEquals(testRow[10], rs.getLong(11));//BIGINT
        assertEquals((Float)testRow[11], rs.getFloat(12), 1e-10f);//REAL
        assertEquals((Double)testRow[12], rs.getDouble(13), 1e-10d);//FLOAT
        assertTrue(Arrays.equals((byte[])testRow[13], rs.getBytes(14)));//BINARY
        assertTrue(Arrays.equals((byte[])testRow[14], rs.getBytes(15)));//VARBINARY
        assertTrue(Arrays.equals((byte[])testRow[15], rs.getBytes(16)));//LONGVARBINARY

        //Issue#44
        //assertEquals(testRow[16], rs.getDate(17));//DATE
        //assertEquals(testRow[17], rs.getTime(18));//TIME
        assertEquals(testRow[18], rs.getTimestamp(19)); //TIMESTAMP

        rs.close();
    }

    @Test
    public void testGetColumnByName() throws SQLException {
        ResultSet rs = stmt.executeQuery("SELECT * FROM test_types");
        assertNotNull(rs);
        assertTrue(rs.next());

        assertEquals(testRow[0], rs.getInt("F1"));//INT
        assertEquals(testRow[1], rs.getString("F2"));//CHAR
        assertEquals(testRow[2], rs.getString("F3"));//VARCHAR
        assertEquals(testRow[3], rs.getString("F4")); //LONGVARCHAR
        assertEquals(testRow[4], rs.getBigDecimal("F5"));// NUMERIC
        assertEquals(testRow[5], rs.getBigDecimal("F6"));// DECIMAL
        assertEquals(testRow[6], rs.getBoolean("F7"));//BIT
        assertEquals(testRow[7], rs.getByte("F8"));//TINYINT
        assertEquals(testRow[8], rs.getShort("F9"));//SMALLINT
        assertEquals(testRow[9], rs.getInt("F10"));//INTEGER
        assertEquals(testRow[10], rs.getLong("F11"));//BIGINT
        assertEquals((Float)testRow[11], rs.getFloat("F12"), 1e-10f);//REAL
        assertEquals((Double)testRow[12], rs.getDouble("F13"), 1e-10d);//FLOAT
        assertTrue(Arrays.equals((byte[])testRow[13], rs.getBytes("F14")));//BINARY
        assertTrue(Arrays.equals((byte[])testRow[14], rs.getBytes("F15")));//VARBINARY
        assertTrue(Arrays.equals((byte[])testRow[15], rs.getBytes("F16")));//LONGVARBINARY

        //Issue#44
        //assertEquals(testRow[16], rs.getDate("F17"));//DATE
        //assertEquals(testRow[17], rs.getTime("F18"));//TIME
        assertEquals(testRow[18], rs.getTimestamp("F19")); //TIMESTAMP

        rs.close();
    }
}
