package org.tarantool.jdbc;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class JdbcStatementIT extends AbstractJdbcIT {
    private Statement stmt;

    @BeforeEach
    public void setUp() throws SQLException {
        stmt = conn.createStatement();
    }

    @AfterEach
    public void tearDown() throws SQLException {
        if (stmt != null && !stmt.isClosed())
            stmt.close();
    }

    @Test
    public void testExecuteQuery() throws SQLException {
        ResultSet rs = stmt.executeQuery("SELECT val FROM test WHERE id=1");
        assertNotNull(rs);
        assertTrue(rs.next());
        assertEquals("one", rs.getString(1));
        assertFalse(rs.next());
        rs.close();
    }

    @Test
    public void testExecuteUpdate() throws Exception {
        assertEquals(2, stmt.executeUpdate("INSERT INTO test(id, val) VALUES (10, 'ten'), (20, 'twenty')"));
        assertEquals("ten", getRow("test", 10).get(1));
        assertEquals("twenty", getRow("test", 20).get(1));
    }

    @Test
    public void testExecuteReturnsResultSet() throws SQLException {
        assertTrue(stmt.execute("SELECT val FROM test WHERE id=1"));
        ResultSet rs = stmt.getResultSet();
        assertNotNull(rs);
        assertTrue(rs.next());
        assertEquals("one", rs.getString(1));
        assertFalse(rs.next());
        rs.close();
    }

    @Test
    public void testExecuteReturnsUpdateCount() throws Exception {
        assertFalse(stmt.execute("INSERT INTO test(id, val) VALUES (100, 'hundred'), (1000, 'thousand')"));
        assertEquals(2, stmt.getUpdateCount());

        assertEquals("hundred", getRow("test", 100).get(1));
        assertEquals("thousand", getRow("test", 1000).get(1));
    }
}