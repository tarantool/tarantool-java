package org.tarantool.jdbc;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Matchers.anyObject;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.tarantool.jdbc.SQLConnection.SQLTarantoolClientImpl;
import static org.tarantool.jdbc.SQLDatabaseMetadata.FORMAT_IDX;
import static org.tarantool.jdbc.SQLDatabaseMetadata.INDEX_FORMAT_IDX;
import static org.tarantool.jdbc.SQLDatabaseMetadata.SPACES_MAX;
import static org.tarantool.jdbc.SQLDatabaseMetadata.SPACE_ID_IDX;
import static org.tarantool.jdbc.SQLDatabaseMetadata._VINDEX;
import static org.tarantool.jdbc.SQLDatabaseMetadata._VSPACE;

import org.tarantool.CommunicationException;
import org.tarantool.TarantoolClientOps;
import org.tarantool.TarantoolClusterClientConfig;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;
import org.junit.jupiter.api.function.ThrowingConsumer;

import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Properties;

public class JdbcExceptionHandlingTest {

    /**
     * Simulates meta parsing error: missing "name" field in a space format for the primary key.
     *
     * @throws SQLException on failure.
     */
    @Test
    public void testDatabaseMetaDataGetPrimaryKeysFormatError() throws SQLException {

        TarantoolClientOps<Integer, List<?>, Object, List<?>> syncOps = mock(TarantoolClientOps.class);

        Object[] spc = new Object[7];
        spc[FORMAT_IDX] = Collections.singletonList(new HashMap<String, Object>());
        spc[SPACE_ID_IDX] = 1000;
        doReturn(Collections.singletonList(Arrays.asList(spc))).when(syncOps)
            .select(_VSPACE, 2, Collections.singletonList("TEST"), 0, 1, 0);

        Object[] idx = new Object[6];
        idx[INDEX_FORMAT_IDX] = Collections.singletonList(
            new HashMap<String, Object>() {
                {
                    put("field", 0);
                }
            }
        );
        doReturn(Collections.singletonList(Arrays.asList(idx))).when(syncOps)
            .select(_VINDEX, 0, Arrays.asList(1000, 0), 0, 1, 0);

        final SQLTarantoolClientImpl client = buildSQLClient(null, syncOps);
        final SQLConnection conn = buildTestSQLConnection(client, "");

        final DatabaseMetaData meta = conn.getMetaData();

        Throwable t = assertThrows(SQLException.class, new Executable() {
            @Override
            public void execute() throws Throwable {
                meta.getPrimaryKeys(null, null, "TEST");
            }
        }, "Error processing metadata for table \"TEST\".");

        assertTrue(t.getCause().getMessage().contains("Wrong value type"));
    }

    @Test
    public void testStatementCommunicationException() throws SQLException {
        checkStatementCommunicationException(new ThrowingConsumer<Statement>() {
            @Override
            public void accept(Statement statement) throws Throwable {
                statement.executeQuery("TEST");
            }
        });
        checkStatementCommunicationException(new ThrowingConsumer<Statement>() {
            @Override
            public void accept(Statement statement) throws Throwable {
                statement.executeUpdate("TEST");
            }
        });
        checkStatementCommunicationException(new ThrowingConsumer<Statement>() {
            @Override
            public void accept(Statement statement) throws Throwable {
                statement.execute("TEST");
            }
        });
    }

    @Test
    public void testPreparedStatementCommunicationException() throws SQLException {
        checkPreparedStatementCommunicationException(new ThrowingConsumer<PreparedStatement>() {
            @Override
            public void accept(PreparedStatement prep) throws Throwable {
                prep.executeQuery();
            }
        });
        checkPreparedStatementCommunicationException(new ThrowingConsumer<PreparedStatement>() {
            @Override
            public void accept(PreparedStatement prep) throws Throwable {
                prep.executeUpdate();
            }
        });
        checkPreparedStatementCommunicationException(new ThrowingConsumer<PreparedStatement>() {
            @Override
            public void accept(PreparedStatement prep) throws Throwable {
                prep.execute();
            }
        });
    }

    @Test
    public void testDatabaseMetaDataCommunicationException() throws SQLException {
        checkDatabaseMetaDataCommunicationException(new ThrowingConsumer<DatabaseMetaData>() {
            @Override
            public void accept(DatabaseMetaData meta) throws Throwable {
                meta.getTables(null, null, null, new String[] { "TABLE" });
            }
        }, "Failed to retrieve table(s) description: tableNamePattern=\"null\".");

        checkDatabaseMetaDataCommunicationException(new ThrowingConsumer<DatabaseMetaData>() {
            @Override
            public void accept(DatabaseMetaData meta) throws Throwable {
                meta.getColumns(null, null, "TEST", "ID");
            }
        }, "Error processing table column metadata: tableNamePattern=\"TEST\"; columnNamePattern=\"ID\".");

        checkDatabaseMetaDataCommunicationException(new ThrowingConsumer<DatabaseMetaData>() {
            @Override
            public void accept(DatabaseMetaData meta) throws Throwable {
                meta.getPrimaryKeys(null, null, "TEST");
            }
        }, "Error processing metadata for table \"TEST\".");
    }

    private void checkStatementCommunicationException(final ThrowingConsumer<Statement> consumer)
        throws SQLException {
        Exception ex = new CommunicationException("TEST");
        SQLTarantoolClientImpl.SQLRawOps sqlOps = mock(SQLTarantoolClientImpl.SQLRawOps.class);
        doThrow(ex).when(sqlOps).execute(anyObject());

        SQLTarantoolClientImpl client = buildSQLClient(sqlOps, null);
        final Statement stmt = new SQLStatement(buildTestSQLConnection(client, "jdbc:tarantool://0:0"));

        SQLException e = assertThrows(SQLException.class, () -> consumer.accept(stmt));
        assertTrue(e.getMessage().startsWith("Failed to execute"), e.getMessage());

        assertEquals(ex, e.getCause());

        verify(client, times(1)).close();
    }

    private void checkPreparedStatementCommunicationException(final ThrowingConsumer<PreparedStatement> consumer)
        throws SQLException {
        Exception ex = new CommunicationException("TEST");
        SQLTarantoolClientImpl.SQLRawOps sqlOps = mock(SQLTarantoolClientImpl.SQLRawOps.class);
        doThrow(ex).when(sqlOps).execute(anyObject());

        SQLTarantoolClientImpl client = buildSQLClient(sqlOps, null);
        final PreparedStatement prep = new SQLPreparedStatement(
            buildTestSQLConnection(client, "jdbc:tarantool://0:0"),
            "TEST",
            Statement.NO_GENERATED_KEYS
        );


        SQLException e = assertThrows(SQLException.class, new Executable() {
            @Override
            public void execute() throws Throwable {
                consumer.accept(prep);
            }
        });
        assertTrue(e.getMessage().startsWith("Failed to execute"), e.getMessage());

        assertEquals(ex, e.getCause());

        verify(client, times(1)).close();
    }

    private void checkDatabaseMetaDataCommunicationException(final ThrowingConsumer<DatabaseMetaData> consumer,
                                                             String msg) throws SQLException {
        Exception ex = new CommunicationException("TEST");
        TarantoolClientOps<Integer, List<?>, Object, List<?>> syncOps = mock(TarantoolClientOps.class);
        doThrow(ex).when(syncOps).select(_VSPACE, 0, Arrays.asList(), 0, SPACES_MAX, 0);
        doThrow(ex).when(syncOps).select(_VSPACE, 2, Arrays.asList("TEST"), 0, 1, 0);

        SQLTarantoolClientImpl client = buildSQLClient(null, syncOps);
        SQLConnection conn = buildTestSQLConnection(client, "jdbc:tarantool://0:0");
        final DatabaseMetaData meta = conn.getMetaData();

        SQLException e = assertThrows(SQLException.class, () -> consumer.accept(meta));
        assertTrue(e.getMessage().startsWith(msg), e.getMessage());

        assertEquals(ex, e.getCause().getCause());

        verify(client, times(1)).close();
    }

    private SQLTarantoolClientImpl buildSQLClient(SQLTarantoolClientImpl.SQLRawOps sqlOps,
                                                  TarantoolClientOps<Integer, List<?>, Object, List<?>> ops) {
        SQLTarantoolClientImpl client = mock(SQLTarantoolClientImpl.class);
        when(client.sqlRawOps()).thenReturn(sqlOps);
        when(client.syncOps()).thenReturn(ops);
        return client;
    }

    private SQLConnection buildTestSQLConnection(SQLTarantoolClientImpl client, String url) throws SQLException {
        return buildTestSQLConnection(client, url, new Properties());
    }

    private SQLConnection buildTestSQLConnection(SQLTarantoolClientImpl client,
                                                 String url,
                                                 Properties properties)
        throws SQLException {
        return new SQLConnection(url, Collections.emptyList(), properties) {
            @Override
            protected SQLTarantoolClientImpl makeSqlClient(List<String> addresses,
                                                           TarantoolClusterClientConfig config) {
                return client;
            }
        };
    }

}
