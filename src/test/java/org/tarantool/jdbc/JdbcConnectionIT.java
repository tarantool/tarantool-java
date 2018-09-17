package org.tarantool.jdbc;

import org.junit.jupiter.api.Test;

import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class JdbcConnectionIT extends AbstractJdbcIT {
    @Test
    public void testCreateStatement() throws SQLException {
        Statement stmt = conn.createStatement();
        assertNotNull(stmt);
        stmt.close();
    }

    @Test
    public void testPrepareStatement() throws SQLException {
        PreparedStatement prep = conn.prepareStatement("INSERT INTO test(id, val) VALUES(?, ?)");
        assertNotNull(prep);
        prep.close();
    }

    @Test
    public void testCloseIsClosed() throws SQLException {
        assertFalse(conn.isClosed());
        conn.close();
        assertTrue(conn.isClosed());
        conn.close();
    }

    @Test
    public void testGetMetaData() throws SQLException {
        DatabaseMetaData meta = conn.getMetaData();
        assertNotNull(meta);
    }
}