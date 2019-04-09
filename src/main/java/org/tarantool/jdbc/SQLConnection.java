package org.tarantool.jdbc;

import static org.tarantool.jdbc.SQLDriver.PROP_HOST;
import static org.tarantool.jdbc.SQLDriver.PROP_PASSWORD;
import static org.tarantool.jdbc.SQLDriver.PROP_PORT;
import static org.tarantool.jdbc.SQLDriver.PROP_SOCKET_TIMEOUT;
import static org.tarantool.jdbc.SQLDriver.PROP_USER;

import org.tarantool.CommunicationException;
import org.tarantool.JDBCBridge;
import org.tarantool.TarantoolConnection;
import org.tarantool.util.SQLStates;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketException;
import java.sql.Array;
import java.sql.Blob;
import java.sql.CallableStatement;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.NClob;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLClientInfoException;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLNonTransientConnectionException;
import java.sql.SQLNonTransientException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Savepoint;
import java.sql.Statement;
import java.sql.Struct;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executor;

@SuppressWarnings("Since15")
public class SQLConnection implements Connection {

    private static final int UNSET_HOLDABILITY = 0;
    private static final String PING_QUERY = "SELECT 1";

    private final TarantoolConnection connection;

    private final String url;
    private final Properties properties;

    private DatabaseMetaData cachedMetadata;

    private int resultSetHoldability = UNSET_HOLDABILITY;

    SQLConnection(String url, Properties properties) throws SQLException {
        this.url = url;
        this.properties = properties;

        String user = properties.getProperty(PROP_USER);
        String pass = properties.getProperty(PROP_PASSWORD);
        Socket socket = null;
        try {
            socket = getConnectedSocket();
            this.connection = makeConnection(user, pass, socket);
        } catch (Exception e) {
            if (socket != null) {
                try {
                    socket.close();
                } catch (IOException ignored) {
                    // No-op.
                }
            }
            if (e instanceof SQLException) {
                throw (SQLException) e;
            }
            throw new SQLException("Couldn't initiate connection using " + SQLDriver.diagProperties(properties), e);
        }
    }

    /**
     * Provides a connected socket to be used to initialize a native tarantool
     * connection.
     * <p>
     * The implementation assumes that {@link #properties} contains all the
     * necessary info extracted from both the URI and connection properties
     * provided by the user. However, the overrides are free to also use the
     * {@link #url} if required.
     * <p>
     * A connect is guarded with user provided timeout. Socket is configured
     * to honor this timeout for the following read/write operations as well.
     *
     * @return Connected socket.
     *
     * @throws SQLException if failed.
     */
    protected Socket getConnectedSocket() throws SQLException {
        Socket socket = makeSocket();
        int timeout = Integer.parseInt(properties.getProperty(PROP_SOCKET_TIMEOUT));
        String host = properties.getProperty(PROP_HOST);
        int port = Integer.parseInt(properties.getProperty(PROP_PORT));
        try {
            socket.connect(new InetSocketAddress(host, port), timeout);
        } catch (IOException e) {
            throw new SQLException("Couldn't connect to " + host + ":" + port, e);
        }
        // Setup socket further.
        if (timeout > 0) {
            try {
                socket.setSoTimeout(timeout);
            } catch (SocketException e) {
                try {
                    socket.close();
                } catch (IOException ignored) {
                    // No-op.
                }
                throw new SQLException("Couldn't set socket timeout. timeout=" + timeout, e);
            }
        }
        return socket;
    }

    /**
     * Provides a newly connected socket instance. The method is intended to be
     * overridden to enable unit testing of the class.
     * <p>
     * Not supposed to contain any logic other than a call to constructor.
     *
     * @return socket.
     */
    protected Socket makeSocket() {
        return new Socket();
    }

    /**
     * Provides a native tarantool connection instance. The method is intended
     * to be overridden to enable unit testing of the class.
     * <p>
     * Not supposed to contain any logic other than a call to constructor.
     *
     * @param user   User name.
     * @param pass   Password.
     * @param socket Connected socket.
     *
     * @return Native tarantool connection.
     *
     * @throws IOException if failed.
     */
    protected TarantoolConnection makeConnection(String user, String pass, Socket socket) throws IOException {
        return new TarantoolConnection(user, pass, socket) {
            {
                msgPackLite = SQLMsgPackLite.INSTANCE;
            }
        };
    }

    @Override
    public Statement createStatement() throws SQLException {
        return createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
    }

    @Override
    public Statement createStatement(int resultSetType, int resultSetConcurrency) throws SQLException {
        return createStatement(resultSetType, resultSetConcurrency, getHoldability());
    }

    @Override
    public Statement createStatement(int resultSetType,
                                     int resultSetConcurrency,
                                     int resultSetHoldability) throws SQLException {
        checkNotClosed();
        checkHoldabilitySupport(resultSetHoldability);
        return new SQLStatement(this, resultSetType, resultSetConcurrency, resultSetHoldability);
    }

    @Override
    public PreparedStatement prepareStatement(String sql) throws SQLException {
        return prepareStatement(sql, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency)
        throws SQLException {
        return prepareStatement(sql, resultSetType, resultSetConcurrency, getHoldability());
    }

    @Override
    public PreparedStatement prepareStatement(String sql,
                                              int resultSetType,
                                              int resultSetConcurrency,
                                              int resultSetHoldability) throws SQLException {
        checkNotClosed();
        checkHoldabilitySupport(resultSetHoldability);
        return new SQLPreparedStatement(this, sql, resultSetType, resultSetConcurrency, resultSetHoldability);
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int[] columnIndexes) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public PreparedStatement prepareStatement(String sql, String[] columnNames) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public CallableStatement prepareCall(String sql) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public CallableStatement prepareCall(String sql,
                                         int resultSetType,
                                         int resultSetConcurrency,
                                         int resultSetHoldability)
        throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public String nativeSQL(String sql) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setAutoCommit(boolean autoCommit) throws SQLException {
        if (autoCommit == false) {
            throw new SQLFeatureNotSupportedException();
        }
    }

    @Override
    public boolean getAutoCommit() throws SQLException {
        return true;
    }

    @Override
    public void commit() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void rollback() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void rollback(Savepoint savepoint) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void close() throws SQLException {
        connection.close();
    }

    @Override
    public boolean isClosed() throws SQLException {
        return connection.isClosed();
    }

    @Override
    public DatabaseMetaData getMetaData() throws SQLException {
        checkNotClosed();
        if (cachedMetadata == null) {
            cachedMetadata = new SQLDatabaseMetadata(this);
        }
        return cachedMetadata;
    }

    @Override
    public void setReadOnly(boolean readOnly) throws SQLException {

    }

    @Override
    public boolean isReadOnly() throws SQLException {
        return false;
    }

    @Override
    public void setCatalog(String catalog) throws SQLException {
    }

    @Override
    public String getCatalog() throws SQLException {
        return null;
    }

    @Override
    public void setTransactionIsolation(int level) throws SQLException {
        if (level != Connection.TRANSACTION_NONE) {
            throw new SQLFeatureNotSupportedException();
        }
    }

    @Override
    public int getTransactionIsolation() throws SQLException {
        return Connection.TRANSACTION_NONE;
    }

    @Override
    public SQLWarning getWarnings() throws SQLException {
        return null;
    }

    @Override
    public void clearWarnings() throws SQLException {

    }

    @Override
    public Map<String, Class<?>> getTypeMap() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setTypeMap(Map<String, Class<?>> map) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setHoldability(int holdability) throws SQLException {
        checkNotClosed();
        checkHoldabilitySupport(holdability);
        resultSetHoldability = holdability;
    }

    @Override
    public int getHoldability() throws SQLException {
        checkNotClosed();
        if (resultSetHoldability == UNSET_HOLDABILITY) {
            resultSetHoldability = getMetaData().getResultSetHoldability();
        }
        return resultSetHoldability;
    }

    @Override
    public Savepoint setSavepoint() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public Savepoint setSavepoint(String name) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void releaseSavepoint(Savepoint savepoint) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public Clob createClob() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public Blob createBlob() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public NClob createNClob() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public SQLXML createSQLXML() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    /**
     * {@inheritDoc}
     *
     * @param timeout temporally ignored param
     *
     * @return connection activity status
     */
    @Override
    public boolean isValid(int timeout) throws SQLException {
        if (timeout < 0) {
            throw new SQLNonTransientException(
                    "Timeout cannot be negative",
                    SQLStates.INVALID_PARAMETER_VALUE.getSqlState()
            );
        }
        if (isClosed()) {
            return false;
        }
        return checkConnection(timeout);
    }

    private boolean checkConnection(int timeout) {
        ResultSet resultSet = null;
        try (Statement pingStatement = createStatement()) {
            // todo: before use timeout we need to provide query timeout per statement

            resultSet = pingStatement.executeQuery(PING_QUERY);
            boolean isValid = resultSet.next() && resultSet.getInt(1) == 1;
            resultSet.close();

            return isValid;
        } catch (SQLException e) {
            return false;
        }
    }

    @Override
    public void setClientInfo(String name, String value) throws SQLClientInfoException {
        throw new SQLClientInfoException();
    }

    @Override
    public void setClientInfo(Properties properties) throws SQLClientInfoException {
        throw new SQLClientInfoException();
    }

    @Override
    public String getClientInfo(String name) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public Properties getClientInfo() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public Array createArrayOf(String typeName, Object[] elements) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public Struct createStruct(String typeName, Object[] attributes) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setSchema(String schema) throws SQLException {
    }

    @Override
    public String getSchema() throws SQLException {
        return null;
    }

    @Override
    public void abort(Executor executor) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setNetworkTimeout(Executor executor, int milliseconds) throws SQLException {
        checkNotClosed();

        if (milliseconds < 0) {
            throw new SQLException("Network timeout cannot be negative.");
        }

        try {
            connection.setSocketTimeout(milliseconds);
        } catch (SocketException e) {
            throw new SQLException("Failed to set socket timeout: timeout=" + milliseconds, e);
        }
    }

    @Override
    public int getNetworkTimeout() throws SQLException {
        checkNotClosed();
        try {
            return connection.getSocketTimeout();
        } catch (SocketException e) {
            throw new SQLException("Failed to retrieve socket timeout", e);
        }
    }

    @Override
    public <T> T unwrap(Class<T> type) throws SQLException {
        if (isWrapperFor(type)) {
            return type.cast(this);
        }
        throw new SQLNonTransientException("Connection does not wrap " + type.getName());
    }

    @Override
    public boolean isWrapperFor(Class<?> type) throws SQLException {
        return type.isAssignableFrom(this.getClass());
    }

    protected Object execute(String sql, Object... args) throws SQLException {
        checkNotClosed();
        try {
            return JDBCBridge.execute(connection, sql, args);
        } catch (Exception e) {
            handleException(e);
            throw new SQLException(formatError(sql, args), e);
        }
    }

    protected JDBCBridge executeQuery(String sql, Object... args) throws SQLException {
        checkNotClosed();
        try {
            return JDBCBridge.query(connection, sql, args);
        } catch (Exception e) {
            handleException(e);
            throw new SQLException(formatError(sql, args), e);
        }
    }

    protected int executeUpdate(String sql, Object... args) throws SQLException {
        checkNotClosed();
        try {
            return JDBCBridge.update(connection, sql, args);
        } catch (Exception e) {
            handleException(e);
            throw new SQLException(formatError(sql, args), e);
        }
    }

    protected List<?> nativeSelect(Integer space, Integer index, List<?> key, int offset, int limit, int iterator)
        throws SQLException {
        checkNotClosed();
        try {
            return connection.select(space, index, key, offset, limit, iterator);
        } catch (Exception e) {
            handleException(e);
            throw new SQLException(e);
        }
    }

    protected String getServerVersion() {
        return connection.getServerVersion();
    }

    /**
     * Checks connection close status.
     *
     * @throws SQLException If connection is closed.
     */
    protected void checkNotClosed() throws SQLException {
        if (isClosed()) {
            throw new SQLNonTransientConnectionException(
                "Connection is closed.",
                SQLStates.CONNECTION_DOES_NOT_EXIST.getSqlState()
            );
        }
    }

    String getUrl() {
        return url;
    }

    Properties getProperties() {
        return properties;
    }

    /**
     * Inspects passed exception and closes the connection if appropriate.
     *
     * @param e Exception to process.
     */
    private void handleException(Exception e) {
        if (CommunicationException.class.isAssignableFrom(e.getClass()) ||
            IOException.class.isAssignableFrom(e.getClass())) {
            try {
                close();
            } catch (SQLException ignored) {
                // No-op.
            }
        }
    }

    /**
     * Checks whether <code>holdability</code> is supported.
     *
     * @param holdability param to be checked
     *
     * @throws SQLFeatureNotSupportedException param is not supported
     * @throws SQLNonTransientException        param has invalid value
     */
    private void checkHoldabilitySupport(int holdability) throws SQLException {
        if (holdability != ResultSet.CLOSE_CURSORS_AT_COMMIT &&
            holdability != ResultSet.HOLD_CURSORS_OVER_COMMIT) {
            throw new SQLNonTransientException("", SQLStates.INVALID_PARAMETER_VALUE.getSqlState());
        }
        if (!getMetaData().supportsResultSetHoldability(holdability)) {
            throw new SQLFeatureNotSupportedException();
        }
    }

    /**
     * Provides error message that contains parameters of failed SQL statement.
     *
     * @param sql    SQL Text.
     * @param params Parameters of the SQL statement.
     *
     * @return Formatted error message.
     */
    private static String formatError(String sql, Object... params) {
        return "Failed to execute SQL: " + sql + ", params: " + Arrays.deepToString(params);
    }
}
