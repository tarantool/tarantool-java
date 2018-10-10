package org.tarantool.jdbc;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;
import org.tarantool.TarantoolConnection;

import java.sql.DatabaseMetaData;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.tarantool.jdbc.SQLDatabaseMetadata.FORMAT_IDX;
import static org.tarantool.jdbc.SQLDatabaseMetadata.INDEX_FORMAT_IDX;
import static org.tarantool.jdbc.SQLDatabaseMetadata.SPACE_ID_IDX;
import static org.tarantool.jdbc.SQLDatabaseMetadata._VINDEX;
import static org.tarantool.jdbc.SQLDatabaseMetadata._VSPACE;

public class JdbcExceptionHandlingTest {
    /**
     * Simulates meta parsing error: missing "name" field in a space format for the primary key.
     *
     * @throws SQLException on failure.
     */
    @Test
    public void testDatabaseMetaDataGetPrimaryKeysFormatError() throws SQLException {
        TarantoolConnection tntCon = mock(TarantoolConnection.class);
        SQLConnection conn = new SQLConnection(tntCon, "", new Properties());

        Object[] spc = new Object[7];
        spc[FORMAT_IDX] = Collections.singletonList(new HashMap<String, Object>());
        spc[SPACE_ID_IDX] = 1000;

        doReturn(Collections.singletonList(Arrays.asList(spc))).when(tntCon)
                .select(_VSPACE, 2, Collections.singletonList("TEST"), 0, 1, 0);

        Object[] idx = new Object[6];
        idx[INDEX_FORMAT_IDX] = Collections.singletonList(
                new HashMap<String, Object>() {{ put("field", 0);}});

        doReturn(Collections.singletonList(Arrays.asList(idx))).when(tntCon)
                .select(_VINDEX, 0, Arrays.asList(1000, 0), 0, 1, 0);

        final DatabaseMetaData meta = conn.getMetaData();

        Throwable t = assertThrows(SQLException.class, new Executable() {
            @Override
            public void execute() throws Throwable {
                meta.getPrimaryKeys(null, null, "TEST");
            }
        }, "Error processing metadata for table \"TEST\".");

        assertTrue(t.getCause().getMessage().contains("Wrong value type"));
    }
}
