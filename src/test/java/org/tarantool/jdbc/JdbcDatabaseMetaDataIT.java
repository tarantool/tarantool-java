package org.tarantool.jdbc;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeEach;

import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class JdbcDatabaseMetaDataIT extends AbstractJdbcIT {
    private DatabaseMetaData meta;

    @BeforeEach
    public void setUp() throws SQLException {
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

        assertTrue(rs.next());
        assertEquals("TEST_COMPOUND", rs.getString("TABLE_NAME"));

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

    @Test
    public void testGetPrimaryKeys() throws SQLException {
        ResultSet rs = meta.getPrimaryKeys(null, null, "TEST");

        assertNotNull(rs);
        assertTrue(rs.next());

        checkGetPrimaryKeysRow(rs, "TEST", "ID", "pk_unnamed_TEST_1", 1);

        assertFalse(rs.next());

        rs.close();
    }

    @Test
    public void testGetPrimaryKeysCompound() throws SQLException {
        ResultSet rs = meta.getPrimaryKeys(null, null, "TEST_COMPOUND");

        assertNotNull(rs);
        assertTrue(rs.next());
        checkGetPrimaryKeysRow(rs, "TEST_COMPOUND", "ID1", "pk_unnamed_TEST_COMPOUND_1", 2);

        assertTrue(rs.next());
        checkGetPrimaryKeysRow(rs, "TEST_COMPOUND", "ID2", "pk_unnamed_TEST_COMPOUND_1", 1);

        assertFalse(rs.next());

        rs.close();
    }

    @Test
    public void testGetPrimaryKeysIgnoresCatalogSchema() throws SQLException {
        String[] vals = new String[] {null, "", "IGNORE"};
        for (String cat : vals) {
            for (String schema : vals) {
                ResultSet rs = meta.getPrimaryKeys(cat, schema, "TEST");

                assertNotNull(rs);
                assertTrue(rs.next());
                checkGetPrimaryKeysRow(rs, "TEST", "ID", "pk_unnamed_TEST_1", 1);
                assertFalse(rs.next());
                rs.close();
            }
        }
    }

    @Test
    public void testGetPrimaryKeysNotFound() throws SQLException {
        String[] tables = new String[] {null, "", "NOSUCHTABLE"};
        for (String t : tables) {
            ResultSet rs = meta.getPrimaryKeys(null, null, t);
            assertNotNull(rs);
            assertFalse(rs.next());
            rs.close();
        }
    }

    @Test
    public void testGetPrimaryKeyNonSQLSpace() throws SQLException {
        ResultSet rs = meta.getPrimaryKeys(null, null, "_vspace");
        assertNotNull(rs);
        assertFalse(rs.next());
        rs.close();
    }

    @Test
    public void testGetPrimaryKeysOrderOfColumns() throws SQLException {
        ResultSet rs = meta.getPrimaryKeys(null, null, "TEST");
        assertNotNull(rs);
        ResultSetMetaData rsMeta = rs.getMetaData();
        assertEquals(6, rsMeta.getColumnCount());
        assertEquals("TABLE_CAT", rsMeta.getColumnName(1));
        assertEquals("TABLE_SCHEM", rsMeta.getColumnName(2));
        assertEquals("TABLE_NAME", rsMeta.getColumnName(3));
        assertEquals("COLUMN_NAME", rsMeta.getColumnName(4));
        assertEquals("KEY_SEQ", rsMeta.getColumnName(5));
        assertEquals("PK_NAME", rsMeta.getColumnName(6));
        rs.close();
    }

    private void checkGetPrimaryKeysRow(ResultSet rs, String table, String colName, String pkName, int seq)
        throws SQLException {
        assertNull(rs.getString("TABLE_CAT"));
        assertNull(rs.getString("TABLE_SCHEM"));
        assertEquals(table, rs.getString("TABLE_NAME"));
        assertEquals(colName, rs.getString("COLUMN_NAME"));
        assertEquals(seq, rs.getInt("KEY_SEQ"));
        assertEquals(pkName, rs.getString("PK_NAME"));

        assertNull(rs.getString(1));
        assertNull(rs.getString(2));
        assertEquals(table, rs.getString(3));
        assertEquals(colName, rs.getString(4));
        assertEquals(seq, rs.getInt(5));
        assertEquals(pkName, rs.getString(6));
    }
}
