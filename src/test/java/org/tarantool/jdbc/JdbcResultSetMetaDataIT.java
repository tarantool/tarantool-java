package org.tarantool.jdbc;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;

@DisplayName("A resultSet metadata")
public class JdbcResultSetMetaDataIT extends AbstractJdbcIT {

    @Test
    @DisplayName("returned correct column names")
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

    @Test
    @DisplayName("unwrapped correct")
    public void testUnwrap() throws SQLException {
        try (
                Statement statement = conn.createStatement();
                ResultSet resultSet = statement.executeQuery("SELECT * FROM test")
        ) {
            ResultSetMetaData metaData = resultSet.getMetaData();
            assertEquals(metaData, metaData.unwrap(SQLResultSetMetaData.class));
            assertThrows(SQLException.class, () -> metaData.unwrap(Integer.class));
        }
    }

    @Test
    @DisplayName("checked as a proper wrapper")
    public void testIsWrapperFor() throws SQLException {
        try (
                Statement statement = conn.createStatement();
                ResultSet resultSet = statement.executeQuery("SELECT * FROM test")
        ) {
            ResultSetMetaData metaData = resultSet.getMetaData();
            assertTrue(metaData.isWrapperFor(SQLResultSetMetaData.class));
            assertFalse(metaData.isWrapperFor(Integer.class));
        }
    }

    @Test
    @DisplayName("returned a correct result columns size")
    public void testColumnCount() throws SQLException {
        try (Statement statement = conn.createStatement()) {
            assertNotNull(statement);

            try (ResultSet resultSet = statement.executeQuery("SELECT * FROM test")) {
                assertNotNull(resultSet);
                ResultSetMetaData metaData = resultSet.getMetaData();
                assertEquals(2, metaData.getColumnCount());
            }
            try (ResultSet resultSet = statement.executeQuery("SELECT id, val FROM test")) {
                assertNotNull(resultSet);
                ResultSetMetaData metaData = resultSet.getMetaData();
                assertEquals(2, metaData.getColumnCount());
            }
            try (ResultSet resultSet = statement.executeQuery("SELECT id FROM test")) {
                assertNotNull(resultSet);
                ResultSetMetaData metaData = resultSet.getMetaData();
                assertEquals(1, metaData.getColumnCount());
            }
        }
    }

    @Test
    @DisplayName("returned correct result column aliases")
    public void testColumnAliases() throws SQLException {
        try (Statement statement = conn.createStatement()) {
            assertNotNull(statement);

            try (ResultSet resultSet = statement.executeQuery("SELECT id AS alias_id FROM test")) {
                assertNotNull(resultSet);
                ResultSetMetaData metaData = resultSet.getMetaData();
                assertEquals("ALIAS_ID", metaData.getColumnLabel(1).toUpperCase());
            }
            try (ResultSet resultSet = statement.executeQuery("SELECT val AS alias_val FROM test")) {
                assertNotNull(resultSet);
                ResultSetMetaData metaData = resultSet.getMetaData();
                assertEquals("ALIAS_VAL", metaData.getColumnLabel(1).toUpperCase());
            }
            try (ResultSet resultSet = statement.executeQuery("SELECT * FROM test")) {
                assertNotNull(resultSet);
                ResultSetMetaData metaData = resultSet.getMetaData();
                assertEquals("ID", metaData.getColumnLabel(1).toUpperCase());
                assertEquals("VAL", metaData.getColumnLabel(2).toUpperCase());
            }
        }
    }

    @Test
    @DisplayName("returned an error when column index is out of range")
    public void testWrongColumnAliases() throws SQLException {
        try (Statement statement = conn.createStatement()) {
            assertNotNull(statement);

            try (ResultSet resultSet = statement.executeQuery("SELECT * FROM test")) {
                assertNotNull(resultSet);
                ResultSetMetaData metaData = resultSet.getMetaData();
                int columnsNumber = metaData.getColumnCount();
                assertThrows(SQLException.class, () -> metaData.getColumnLabel(columnsNumber + 1));
                assertThrows(SQLException.class, () -> metaData.getColumnLabel(-5));
                assertThrows(SQLException.class, () -> metaData.getColumnLabel(Integer.MAX_VALUE));
                assertThrows(SQLException.class, () -> metaData.getColumnLabel(Integer.MIN_VALUE));
            }
        }
    }

}
