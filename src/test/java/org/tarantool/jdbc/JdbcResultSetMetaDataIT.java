package org.tarantool.jdbc;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import org.junit.jupiter.api.Test;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;

public class JdbcResultSetMetaDataIT extends AbstractJdbcIT {
    @Test
    public void testColumnNames() throws SQLException {
        Statement stmt = conn.createStatement();
        assertNotNull(stmt);
        ResultSet rs = stmt.executeQuery("SELECT * FROM test_types");
        assertNotNull(rs);

        ResultSetMetaData rsMeta = rs.getMetaData();

        int colCount = 1 + TntSqlType.values().length;
        assertEquals(colCount, rsMeta.getColumnCount());
        assertEquals("KEY", rsMeta.getColumnName(1));

        for (int i = 2; i <= colCount; i++) {
            assertEquals("F" + (i - 2), rsMeta.getColumnName(i));
        }

        rs.close();
        stmt.close();
    }
}
