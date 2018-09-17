package org.tarantool.jdbc;

import org.junit.jupiter.api.Test;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class JdbcResultSetMetaDataIT extends AbstractJdbcIT {
    @Test
    public void testColumnNames() throws SQLException {
        Statement stmt = conn.createStatement();
        assertNotNull(stmt);
        ResultSet rs = stmt.executeQuery("SELECT * FROM test_types");
        assertNotNull(rs);
        assertTrue(rs.next());

        ResultSetMetaData rsMeta = rs.getMetaData();

        assertEquals(19, rsMeta.getColumnCount());

        for (int i = 1; i <= 19; i++)
            assertEquals("F" + i, rsMeta.getColumnName(i));

        rs.close();
        stmt.close();
    }
}
