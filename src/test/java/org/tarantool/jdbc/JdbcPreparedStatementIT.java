package org.tarantool.jdbc;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class JdbcPreparedStatementIT extends JdbcTypesIT {

    private PreparedStatement prep;

    @AfterEach
    public void tearDown() throws SQLException {
        if (prep != null && !prep.isClosed()) {
            prep.close();
        }
    }

    @Test
    public void testExecuteQuery() throws SQLException {
        prep = conn.prepareStatement("SELECT val FROM test WHERE id=?");
        assertNotNull(prep);

        prep.setInt(1, 1);
        ResultSet rs = prep.executeQuery();
        assertNotNull(rs);
        assertTrue(rs.next());
        assertEquals("one", rs.getString(1));
        assertFalse(rs.next());
        rs.close();

        // Reuse the prepared statement.
        prep.setInt(1, 2);
        rs = prep.executeQuery();
        assertNotNull(rs);
        assertTrue(rs.next());
        assertEquals("two", rs.getString(1));
        assertFalse(rs.next());
        rs.close();
    }

    @Test
    public void testExecuteUpdate() throws Exception {
        prep = conn.prepareStatement("INSERT INTO test VALUES(?, ?)");
        assertNotNull(prep);

        prep.setInt(1, 100);
        prep.setString(2, "hundred");
        int count = prep.executeUpdate();
        assertEquals(1, count);

        assertEquals("hundred", getRow("test", 100).get(1));

        // Reuse the prepared statement.
        prep.setInt(1, 1000);
        prep.setString(2, "thousand");
        count = prep.executeUpdate();
        assertEquals(1, count);

        assertEquals("thousand", getRow("test", 1000).get(1));
    }

    @Test
    public void testExecuteReturnsResultSet() throws SQLException {
        prep = conn.prepareStatement("SELECT val FROM test WHERE id=?");
        assertNotNull(prep);
        prep.setInt(1, 1);

        assertTrue(prep.execute());
        assertEquals(-1, prep.getUpdateCount());

        ResultSet rs = prep.getResultSet();
        assertNotNull(rs);
        assertTrue(rs.next());
        assertEquals("one", rs.getString(1));
        assertFalse(rs.next());
        rs.close();
    }

    @Test
    public void testExecuteReturnsUpdateCount() throws Exception {
        prep = conn.prepareStatement("INSERT INTO test VALUES(?, ?), (?, ?)");
        assertNotNull(prep);

        prep.setInt(1, 10);
        prep.setString(2, "ten");
        prep.setInt(3, 20);
        prep.setString(4, "twenty");

        assertFalse(prep.execute());
        assertNull(prep.getResultSet());
        assertEquals(2, prep.getUpdateCount());

        assertEquals("ten", getRow("test", 10).get(1));
        assertEquals("twenty", getRow("test", 20).get(1));

        conn.createStatement().execute("DELETE FROM test WHERE id IN (10, 20)");
    }

    @Test
    void testForbiddenMethods() throws SQLException {
        prep = conn.prepareStatement("TEST");

        int i = 0;
        for (; i < 3; i++) {
            final int step = i;
            SQLException e = assertThrows(SQLException.class, new Executable() {
                @Override
                public void execute() throws Throwable {
                    switch (step) {
                    case 0:
                        prep.executeQuery("TEST");
                        break;
                    case 1:
                        prep.executeUpdate("TEST");
                        break;
                    case 2:
                        prep.execute("TEST");
                        break;
                    default:
                        fail();
                    }
                }
            });
            assertEquals("The method cannot be called on a PreparedStatement.", e.getMessage());
        }
        assertEquals(3, i);
    }

    @Test
    public void testClosedConnection() throws SQLException {
        prep = conn.prepareStatement("TEST");

        conn.close();

        int i = 0;
        for (; i < 3; i++) {
            final int step = i;
            SQLException e = assertThrows(SQLException.class, new Executable() {
                @Override
                public void execute() throws Throwable {
                    switch (step) {
                    case 0:
                        prep.executeQuery();
                        break;
                    case 1:
                        prep.executeUpdate();
                        break;
                    case 2:
                        prep.execute();
                        break;
                    default:
                        fail();
                    }
                }
            });
            assertEquals("Statement is closed.", e.getMessage());
        }
        assertEquals(3, i);
    }

    @Test
    public void testUnwrap() throws SQLException {
        prep = conn.prepareStatement("SELECT val FROM test");
        assertEquals(prep, prep.unwrap(SQLPreparedStatement.class));
        assertEquals(prep, prep.unwrap(SQLStatement.class));
        assertThrows(SQLException.class, () -> prep.unwrap(Integer.class));
    }

    @Test
    public void testIsWrapperFor() throws SQLException {
        prep = conn.prepareStatement("SELECT val FROM test");
        assertTrue(prep.isWrapperFor(SQLPreparedStatement.class));
        assertTrue(prep.isWrapperFor(SQLStatement.class));
        assertFalse(prep.isWrapperFor(Integer.class));
    }

    @Test
    public void testSupportGeneratedKeys() throws SQLException {
        prep = conn.prepareStatement("INSERT INTO test values (50, 'fifty')", Statement.NO_GENERATED_KEYS);
        assertFalse(prep.execute());
        assertEquals(1, prep.getUpdateCount());

        ResultSet generatedKeys = prep.getGeneratedKeys();
        assertNotNull(generatedKeys);
        assertEquals(ResultSet.TYPE_FORWARD_ONLY, generatedKeys.getType());
        assertEquals(ResultSet.CONCUR_READ_ONLY, generatedKeys.getConcurrency());
    }

    @Test
    public void testSetByte() throws SQLException {
        makeHelper(Byte.class)
            .setColumns(TntSqlType.INT, TntSqlType.INTEGER)
            .setValues(BYTE_VALS)
            .testSetParameter();
    }

    @Test
    public void testSetInt() throws SQLException {
        makeHelper(Integer.class)
            .setColumns(TntSqlType.INT, TntSqlType.INTEGER)
            .setValues(INT_VALS)
            .testSetParameter();
    }

    @Test
    public void testSetLong() throws SQLException {
        makeHelper(Long.class)
            .setColumns(TntSqlType.INT, TntSqlType.INTEGER)
            .setValues(LONG_VALS)
            .testSetParameter();
    }

    @Test
    public void testSetString() throws SQLException {
        makeHelper(String.class)
            .setColumns(TntSqlType.VARCHAR, TntSqlType.TEXT)
            .setValues(STRING_VALS)
            .testSetParameter();
    }

    @Test
    public void testSetFloat() throws SQLException {
        makeHelper(Float.class)
            .setColumns(TntSqlType.REAL)
            .setValues(FLOAT_VALS)
            .testSetParameter();
    }

    @Test
    public void testSetDouble() throws SQLException {
        makeHelper(Double.class)
            .setColumns(TntSqlType.FLOAT, TntSqlType.DOUBLE)
            .setValues(DOUBLE_VALS)
            .testSetParameter();
    }

    @Test
    public void testSetBigDecimal() throws SQLException {
        makeHelper(BigDecimal.class)
            .setColumns(TntSqlType.REAL, TntSqlType.FLOAT, TntSqlType.DOUBLE)
            .setValues(BIGDEC_VALS)
            .testSetParameter();
    }

    @Test
    public void testSetByteArray() throws SQLException {
        makeHelper(byte[].class)
            .setColumns(TntSqlType.SCALAR)
            .setValues(BINARY_VALS)
            .testSetParameter();
    }

    @Test
    public void testSetDate() throws SQLException {
        makeHelper(Date.class)
            .setColumns(TntSqlType.INT, TntSqlType.INTEGER)
            .setValues(DATE_VALS)
            .testSetParameter();
    }

}
