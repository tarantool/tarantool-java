package org.tarantool.jdbc;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeEach;

import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class JdbcDatabaseMetaDataIT extends AbstractJdbcIT {
    private DatabaseMetaData meta;

    @BeforeEach
    public void setUp() throws Exception {
        meta = conn.getMetaData();
    }

    @Test
    public void testGetTableTypes() throws SQLException {
        ResultSet rs = meta.getTableTypes();
        assertNotNull(rs);

        assertTrue(rs.next());
        assertEquals("TABLE", rs.getString("TABLE_TYPE"));
        assertFalse(rs.next());

        rs.close();
    }

    @Test
    public void testGetAllTables() throws SQLException {
        ResultSet rs = meta.getTables(null, null, null, new String[] {"TABLE"});
        assertNotNull(rs);

        assertTrue(rs.next());
        assertEquals("TEST", rs.getString("TABLE_NAME"));

        assertTrue(rs.next());
        assertEquals("TEST_TYPES", rs.getString("TABLE_NAME"));

        assertFalse(rs.next());

        rs.close();
    }

    @Test
    public void testGetTable() throws SQLException {
        ResultSet rs = meta.getTables(null, null, "TEST", new String[] {"TABLE"});
        assertNotNull(rs);
        assertTrue(rs.next());
        assertEquals("TEST", rs.getString("TABLE_NAME"));

        assertFalse(rs.next());

        rs.close();
    }

    @Test
    public void testGetColumns() throws SQLException {
        ResultSet rs = meta.getColumns(null, null, "TEST", null);
        assertNotNull(rs);

        assertTrue(rs.next());

        assertEquals("TEST", rs.getString("TABLE_NAME"));
        assertEquals("ID", rs.getString("COLUMN_NAME"));
        assertEquals(1, rs.getInt("ORDINAL_POSITION"));

        assertTrue(rs.next());

        assertEquals("TEST", rs.getString("TABLE_NAME"));
        assertEquals("VAL", rs.getString("COLUMN_NAME"));
        assertEquals(2, rs.getInt("ORDINAL_POSITION"));

        assertFalse(rs.next());

        rs.close();
    }

    @Disabled(value="Test ignored, issue#41")
    @Test
    public void testGetPrimaryKeys() throws SQLException {
        ResultSet rs = meta.getPrimaryKeys(null, null, "TEST");

        assertNotNull(rs);
        assertTrue(rs.next());

        assertNull(rs.getString("TABLE_CAT"));
        assertNull(rs.getString("TABLE_SCHEM"));
        assertEquals("TEST", rs.getString("TABLE_NAME"));
        assertEquals("ID", rs.getString("COLUMN_NAME"));
        assertEquals(1, rs.getInt("KEY_SEQ"));
        assertEquals("pk_unnamed_TEST_1", rs.getString("PK_NAME"));

        assertFalse(rs.next());

        rs.close();
    }
}
