package org.tarantool.jdbc;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.tarantool.jdbc.SQLDatabaseMetadata.FORMAT_IDX;
import static org.tarantool.jdbc.SQLDatabaseMetadata.INDEX_FORMAT_IDX;
import static org.tarantool.jdbc.SQLDatabaseMetadata.SPACES_MAX;
import static org.tarantool.jdbc.SQLDatabaseMetadata.SPACE_ID_IDX;
import static org.tarantool.jdbc.SQLDatabaseMetadata._VINDEX;
import static org.tarantool.jdbc.SQLDatabaseMetadata._VSPACE;
import static org.tarantool.jdbc.SQLDriver.PROP_SOCKET_TIMEOUT;

import org.tarantool.CommunicationException;
import org.tarantool.TarantoolConnection;
import org.tarantool.protocol.TarantoolPacket;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;
import org.junit.jupiter.api.function.ThrowingConsumer;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Properties;

public class JdbcExceptionHandlingTest {

    /**
     * Simulates meta parsing error: missing "name" field in a space format for the primary key.
     *
     * @throws SQLException on failure.
     */
    @Test
    public void testDatabaseMetaDataGetPrimaryKeysFormatError() throws SQLException {
        TarantoolConnection tntCon = mock(TarantoolConnection.class);
        final SQLConnection conn = buildTestSQLConnection(tntCon, "", SQLDriver.defaults);

        Object[] spc = new Object[7];
        spc[FORMAT_IDX] = Collections.singletonList(new HashMap<String, Object>());
        spc[SPACE_ID_IDX] = 1000;

        doReturn(Collections.singletonList(Arrays.asList(spc))).when(tntCon)
            .select(_VSPACE, 2, Collections.singletonList("TEST"), 0, 1, 0);

        Object[] idx = new Object[6];
        idx[INDEX_FORMAT_IDX] = Collections.singletonList(
            new HashMap<String, Object>() {
                {
                    put("field", 0);
                }
            }
        );

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

    @Test
    public void testDefaultSocketProviderConnectTimeoutError() throws IOException {
        final int socketTimeout = 3000;
        final Socket mockSocket = mock(Socket.class);

        SocketTimeoutException timeoutEx = new SocketTimeoutException();
        doThrow(timeoutEx)
            .when(mockSocket)
            .connect(new InetSocketAddress("localhost", 3301), socketTimeout);

        final Properties prop = new Properties(SQLDriver.defaults);
        prop.setProperty(PROP_SOCKET_TIMEOUT, String.valueOf(socketTimeout));

        SQLException e = assertThrows(SQLException.class, new Executable() {
            @Override
            public void execute() throws Throwable {
                new SQLConnection("tarantool://localhost:3301", prop) {
                    @Override
                    protected Socket makeSocket() {
                        return mockSocket;
                    }
                };
            }
        });

        assertTrue(e.getMessage().startsWith("Couldn't connect to localhost:3301"), e.getMessage());
        assertEquals(timeoutEx, e.getCause());
    }

    @Test
    public void testDefaultSocketProviderSetSocketTimeoutError() throws IOException {
        final int socketTimeout = 3000;
        final Socket mockSocket = mock(Socket.class);

        // Check error setting socket timeout
        reset(mockSocket);
        doNothing()
            .when(mockSocket)
            .connect(new InetSocketAddress("localhost", 3301), socketTimeout);

        SocketException sockEx = new SocketException("TEST");
        doThrow(sockEx)
            .when(mockSocket)
            .setSoTimeout(socketTimeout);

        final Properties prop = new Properties(SQLDriver.defaults);
        prop.setProperty(PROP_SOCKET_TIMEOUT, String.valueOf(socketTimeout));

        SQLException e = assertThrows(SQLException.class, new Executable() {
            @Override
            public void execute() throws Throwable {
                new SQLConnection("tarantool://localhost:3301", prop) {
                    @Override
                    protected Socket makeSocket() {
                        return mockSocket;
                    }
                };
            }
        });

        assertTrue(e.getMessage().startsWith("Couldn't set socket timeout."), e.getMessage());
        assertEquals(sockEx, e.getCause());
    }

    private void checkStatementCommunicationException(final ThrowingConsumer<Statement> consumer)
        throws SQLException {
        TestTarantoolConnection mockCon = mock(TestTarantoolConnection.class);
        final Statement stmt = new SQLStatement(buildTestSQLConnection(mockCon, "tarantool://0:0", SQLDriver.defaults));

        Exception ex = new CommunicationException("TEST");

        doThrow(ex).when(mockCon).sql("TEST", new Object[0]);
        doThrow(ex).when(mockCon).update("TEST");

        SQLException e = assertThrows(SQLException.class, new Executable() {
            @Override
            public void execute() throws Throwable {
                consumer.accept(stmt);
            }
        });
        assertTrue(e.getMessage().startsWith("Failed to execute"), e.getMessage());

        assertEquals(ex, e.getCause());

        verify(mockCon, times(1)).close();
    }

    private void checkPreparedStatementCommunicationException(final ThrowingConsumer<PreparedStatement> consumer)
        throws SQLException {
        TestTarantoolConnection mockCon = mock(TestTarantoolConnection.class);

        final PreparedStatement prep = new SQLPreparedStatement(
            buildTestSQLConnection(mockCon, "tarantool://0:0", SQLDriver.defaults), "TEST");

        Exception ex = new CommunicationException("TEST");
        doThrow(ex).when(mockCon).sql("TEST", new Object[0]);
        doThrow(ex).when(mockCon).update("TEST");

        SQLException e = assertThrows(SQLException.class, new Executable() {
            @Override
            public void execute() throws Throwable {
                consumer.accept(prep);
            }
        });
        assertTrue(e.getMessage().startsWith("Failed to execute"), e.getMessage());

        assertEquals(ex, e.getCause());

        verify(mockCon, times(1)).close();
    }

    private void checkDatabaseMetaDataCommunicationException(final ThrowingConsumer<DatabaseMetaData> consumer,
                                                             String msg) throws SQLException {
        TestTarantoolConnection mockCon = mock(TestTarantoolConnection.class);
        SQLConnection conn = buildTestSQLConnection(mockCon, "tarantool://0:0", new Properties(SQLDriver.defaults));
        final DatabaseMetaData meta = conn.getMetaData();

        Exception ex = new CommunicationException("TEST");
        doThrow(ex).when(mockCon).select(_VSPACE, 0, Arrays.asList(), 0, SPACES_MAX, 0);
        doThrow(ex).when(mockCon).select(_VSPACE, 2, Arrays.asList("TEST"), 0, 1, 0);

        SQLException e = assertThrows(SQLException.class, new Executable() {
            @Override
            public void execute() throws Throwable {
                consumer.accept(meta);
            }
        });
        assertTrue(e.getMessage().startsWith(msg), e.getMessage());

        assertEquals(ex, e.getCause().getCause());

        verify(mockCon, times(1)).close();
    }

    private SQLConnection buildTestSQLConnection(final TarantoolConnection tntCon, String url, Properties properties)
        throws SQLException {
        return new SQLConnection(url, properties) {
            @Override
            protected Socket makeSocket() {
                return mock(Socket.class);
            }

            @Override
            protected TarantoolConnection makeConnection(String user, String pass, Socket socket) {
                return tntCon;
            }
        };
    }

    class TestTarantoolConnection extends TarantoolConnection {
        TestTarantoolConnection() throws IOException {
            super(null, null, mock(Socket.class));
        }

        @Override
        protected TarantoolPacket sql(String sql, Object[] bind) {
            return super.sql(sql, bind);
        }
    }
}
