package org.tarantool.jdbc;

import org.tarantool.Code;
import org.tarantool.CommunicationException;
import org.tarantool.Key;
import org.tarantool.SocketChannelProvider;
import org.tarantool.SqlProtoUtils;
import org.tarantool.TarantoolClientConfig;
import org.tarantool.TarantoolClientImpl;
import org.tarantool.protocol.TarantoolPacket;
import org.tarantool.util.JdbcConstants;
import org.tarantool.util.SQLStates;

import java.io.IOException;
import java.sql.Array;
import java.sql.Blob;
import java.sql.CallableStatement;
import java.sql.ClientInfoStatus;
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
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executor;
import java.util.concurrent.Future;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;

/**
 * Tarantool {@link Connection} implementation.
 * <p>
 * Supports creating {@link Statement} and {@link PreparedStatement} instances
 */
public class SQLConnection implements Connection {

    private static final int UNSET_HOLDABILITY = 0;
    private static final String PING_QUERY = "SELECT 1";

    private final SQLTarantoolClientImpl client;
    private final String url;
    private final Properties properties;
    private DatabaseMetaData cachedMetadata;
    private int resultSetHoldability = UNSET_HOLDABILITY;

    public SQLConnection(String url, Properties properties) throws SQLException {
        this.url = url;
        this.properties = properties;

        try {
            client = makeSqlClient(makeAddress(properties), makeConfigFromProperties(properties));
        } catch (Exception e) {
            throw new SQLException("Couldn't initiate connection using " + SQLDriver.diagProperties(properties), e);
        }
    }

    protected SQLTarantoolClientImpl makeSqlClient(String address, TarantoolClientConfig config) {
        return new SQLTarantoolClientImpl(address, config);
    }

    private String makeAddress(Properties properties) throws SQLException {
        String host = SQLProperty.HOST.getString(properties);
        int port = SQLProperty.PORT.getInt(properties);
        return host + ":" + port;
    }

    private TarantoolClientConfig makeConfigFromProperties(Properties properties) throws SQLException {
        TarantoolClientConfig clientConfig = new TarantoolClientConfig();
        clientConfig.username = SQLProperty.USER.getString(properties);
        clientConfig.password = SQLProperty.PASSWORD.getString(properties);

        clientConfig.operationExpiryTimeMillis = SQLProperty.QUERY_TIMEOUT.getInt(properties);
        clientConfig.initTimeoutMillis = SQLProperty.LOGIN_TIMEOUT.getInt(properties);

        return clientConfig;
    }

    @Override
    public void commit() throws SQLException {
        checkNotClosed();
        if (getAutoCommit()) {
            throw new SQLNonTransientException(
                "Cannot commit when auto-commit is enabled.",
                SQLStates.INVALID_TRANSACTION_STATE.getSqlState()
            );
        }
        throw new SQLFeatureNotSupportedException();
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
        checkStatementParams(resultSetType, resultSetConcurrency, resultSetHoldability);
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
        checkStatementParams(resultSetType, resultSetConcurrency, resultSetHoldability);
        return new SQLPreparedStatement(this, sql, resultSetType, resultSetConcurrency, resultSetHoldability);
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys) throws SQLException {
        checkNotClosed();
        JdbcConstants.checkGeneratedKeysConstant(autoGeneratedKeys);
        if (autoGeneratedKeys != Statement.NO_GENERATED_KEYS) {
            throw new SQLFeatureNotSupportedException();
        }
        return prepareStatement(sql);
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int[] columnIndexes) throws SQLException {
        checkNotClosed();
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public PreparedStatement prepareStatement(String sql, String[] columnNames) throws SQLException {
        checkNotClosed();
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public CallableStatement prepareCall(String sql) throws SQLException {
        return prepareCall(sql, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
    }

    @Override
    public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
        return prepareCall(sql, resultSetType, resultSetConcurrency, getHoldability());
    }

    @Override
    public CallableStatement prepareCall(String sql,
                                         int resultSetType,
                                         int resultSetConcurrency,
                                         int resultSetHoldability)
        throws SQLException {
        checkNotClosed();
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public String nativeSQL(String sql) throws SQLException {
        checkNotClosed();
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setAutoCommit(boolean autoCommit) throws SQLException {
        checkNotClosed();
        if (!autoCommit) {
            throw new SQLFeatureNotSupportedException();
        }
    }

    @Override
    public boolean getAutoCommit() throws SQLException {
        checkNotClosed();
        return true;
    }

    @Override
    public void close() throws SQLException {
        client.close();
    }

    @Override
    public void rollback() throws SQLException {
        checkNotClosed();
        if (getAutoCommit()) {
            throw new SQLNonTransientException(
                "Cannot rollback when auto-commit is enabled.",
                SQLStates.INVALID_TRANSACTION_STATE.getSqlState()
            );
        }
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void rollback(Savepoint savepoint) throws SQLException {
        checkNotClosed();
        if (getAutoCommit()) {
            throw new SQLNonTransientException(
                "Cannot roll back to a savepoint when auto-commit is enabled.",
                SQLStates.INVALID_TRANSACTION_STATE.getSqlState()
            );
        }
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public boolean isClosed() throws SQLException {
        return client.isClosed();
    }

    @Override
    public Savepoint setSavepoint() throws SQLException {
        checkNotClosed();
        if (getAutoCommit()) {
            throw new SQLNonTransientException(
                "Cannot set a savepoint when auto-commit is enabled.",
                SQLStates.INVALID_TRANSACTION_STATE.getSqlState()
            );
        }
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public Savepoint setSavepoint(String name) throws SQLException {
        checkNotClosed();
        if (getAutoCommit()) {
            throw new SQLNonTransientException(
                "Cannot set a savepoint when auto-commit is enabled.",
                SQLStates.INVALID_TRANSACTION_STATE.getSqlState()
            );
        }
        throw new SQLFeatureNotSupportedException();
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
        checkNotClosed();
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public boolean isReadOnly() throws SQLException {
        checkNotClosed();
        return false;
    }

    @Override
    public void setCatalog(String catalog) throws SQLException {
        checkNotClosed();
    }

    @Override
    public String getCatalog() throws SQLException {
        checkNotClosed();
        return null;
    }

    @Override
    public void setTransactionIsolation(int level) throws SQLException {
        checkNotClosed();
        if (level != Connection.TRANSACTION_NONE) {
            throw new SQLFeatureNotSupportedException();
        }
    }

    @Override
    public int getTransactionIsolation() throws SQLException {
        checkNotClosed();
        return Connection.TRANSACTION_NONE;
    }

    @Override
    public SQLWarning getWarnings() throws SQLException {
        checkNotClosed();
        return null;
    }

    @Override
    public void clearWarnings() throws SQLException {
        checkNotClosed();
    }

    @Override
    public Map<String, Class<?>> getTypeMap() throws SQLException {
        checkNotClosed();
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setTypeMap(Map<String, Class<?>> map) throws SQLException {
        checkNotClosed();
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

    /**
     * {@inheritDoc}
     *
     * @param timeout time in seconds
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

    @Override
    public void releaseSavepoint(Savepoint savepoint) throws SQLException {
        checkNotClosed();
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public Clob createClob() throws SQLException {
        checkNotClosed();
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public Blob createBlob() throws SQLException {
        checkNotClosed();
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public NClob createNClob() throws SQLException {
        checkNotClosed();
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public SQLXML createSQLXML() throws SQLException {
        checkNotClosed();
        throw new SQLFeatureNotSupportedException();
    }

    private boolean checkConnection(int timeout) {
        ResultSet resultSet;
        try (Statement pingStatement = createStatement()) {
            pingStatement.setQueryTimeout(timeout);
            resultSet = pingStatement.executeQuery(PING_QUERY);
            boolean isValid = resultSet.next() && resultSet.getInt(1) == 1;
            resultSet.close();

            return isValid;
        } catch (SQLException e) {
            return false;
        }
    }

    @Override
    public void setNetworkTimeout(Executor executor, int milliseconds) throws SQLException {
        checkNotClosed();
        if (milliseconds < 0) {
            throw new SQLException("Network timeout cannot be negative.");
        }
        client.setOperationTimeout(milliseconds);
    }

    @Override
    public void setClientInfo(String name, String value) throws SQLClientInfoException {
        try {
            checkNotClosed();
        } catch (SQLException cause) {
            throwUnknownReasonClientProperties("Connection is closed", Collections.singleton(name), cause);
        }
        throwUnknownClientProperties(Collections.singleton(name));
    }

    @Override
    public void setClientInfo(Properties properties) throws SQLClientInfoException {
        try {
            checkNotClosed();
        } catch (SQLException cause) {
            throwUnknownReasonClientProperties("Connection is closed", properties.keySet(), cause);
        }
        throwUnknownClientProperties(properties.keySet());
    }

    /**
     * Throws an exception caused by {@code cause} and marks all properties
     * as {@link ClientInfoStatus#REASON_UNKNOWN}.
     *
     * @param reason     reason mesage
     * @param properties client properties
     * @param cause      original cause
     *
     * @throws SQLClientInfoException wrapped exception
     */
    private void throwUnknownReasonClientProperties(String reason,
                                                    Collection<Object> properties,
                                                    SQLException cause) throws SQLClientInfoException {
        Map<String, ClientInfoStatus> failedProperties = new HashMap<>();
        properties.forEach(property -> {
            failedProperties.put(property.toString(), ClientInfoStatus.REASON_UNKNOWN);
        });
        throw new SQLClientInfoException(reason, cause.getSQLState(), failedProperties, cause);
    }

    /**
     * Throws exception for unrecognizable properties.
     *
     * @param properties unknown property names.
     *
     * @throws SQLClientInfoException wrapped exception
     */
    private void throwUnknownClientProperties(Collection<Object> properties) throws SQLClientInfoException {
        Map<String, ClientInfoStatus> failedProperties = new HashMap<>();
        properties.forEach(property -> {
            failedProperties.put(property.toString(), ClientInfoStatus.REASON_UNKNOWN_PROPERTY);
        });
        throw new SQLClientInfoException(failedProperties);
    }

    @Override
    public String getClientInfo(String name) throws SQLException {
        checkNotClosed();
        return null;
    }

    @Override
    public Properties getClientInfo() throws SQLException {
        checkNotClosed();
        return new Properties();
    }

    @Override
    public Array createArrayOf(String typeName, Object[] elements) throws SQLException {
        checkNotClosed();
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public Struct createStruct(String typeName, Object[] attributes) throws SQLException {
        checkNotClosed();
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setSchema(String schema) throws SQLException {
        checkNotClosed();
    }

    @Override
    public String getSchema() throws SQLException {
        checkNotClosed();
        return null;
    }

    @Override
    public void abort(Executor executor) throws SQLException {
        if (isClosed()) {
            return;
        }
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public int getNetworkTimeout() throws SQLException {
        checkNotClosed();
        return (int) client.getOperationTimeout();
    }

    protected SQLResultHolder execute(long timeout, SQLQueryHolder query) throws SQLException {
        checkNotClosed();
        return (useNetworkTimeout(timeout))
            ? executeWithNetworkTimeout(query)
            : executeWithQueryTimeout(timeout, query);
    }

    protected SQLBatchResultHolder executeBatch(long timeout, List<SQLQueryHolder> queries) throws SQLException {
        checkNotClosed();
        SQLTarantoolClientImpl.SQLRawOps sqlOps = client.sqlRawOps();
        SQLBatchResultHolder batchResult = useNetworkTimeout(timeout)
             ? sqlOps.executeBatch(queries)
             : sqlOps.executeBatch(timeout, queries);

        return batchResult;
    }

    private boolean useNetworkTimeout(long timeout) throws SQLException {
        int networkTimeout = getNetworkTimeout();
        return timeout == 0 || (networkTimeout > 0 && networkTimeout < timeout);
    }

    private SQLResultHolder executeWithNetworkTimeout(SQLQueryHolder query) throws SQLException {
        try {
            return client.sqlRawOps().execute(query);
        } catch (Exception e) {
            handleException(e);
            throw new SQLException(formatError(query), e);
        }
    }

    /**
     * Executes a query using a custom timeout.
     *
     * @param timeout query timeout
     * @param query   query
     *
     * @return SQL result holder
     *
     * @throws StatementTimeoutException if query execution took more than query timeout
     * @throws SQLException              if any other errors occurred
     */
    private SQLResultHolder executeWithQueryTimeout(long timeout, SQLQueryHolder query) throws SQLException {
        try {
            return client.sqlRawOps().execute(timeout, query);
        } catch (Exception e) {
            // statement timeout should not affect the current connection
            // but can be handled by the caller side
            if (e.getCause() instanceof TimeoutException) {
                throw new StatementTimeoutException(formatError(query), e.getCause());
            }
            handleException(e);
            throw new SQLException(formatError(query), e);
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

    protected List<?> nativeSelect(Integer space, Integer index, List<?> key, int offset, int limit, int iterator)
        throws SQLException {
        checkNotClosed();
        try {
            return client.syncOps().select(space, index, key, offset, limit, iterator);
        } catch (Exception e) {
            handleException(e);
            throw new SQLException(e);
        }
    }

    protected String getServerVersion() {
        return client.getServerVersion();
    }

    /**
     * Inspects passed exception and closes the connection if appropriate.
     *
     * @param e Exception to process.
     */
    private void handleException(Exception e) {
        if (e instanceof CommunicationException ||
            e instanceof IOException ||
            e.getCause() instanceof TimeoutException) {
            try {
                close();
            } catch (SQLException ignored) {
                // No-op.
            }
        }
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
     * Checks all params required to make statements.
     *
     * @param resultSetType        scroll type
     * @param resultSetConcurrency concurrency level
     * @param resultSetHoldability holdability type
     *
     * @throws SQLFeatureNotSupportedException if any param is not supported
     * @throws SQLNonTransientException        if any param has an invalid value
     */
    private void checkStatementParams(int resultSetType,
                                      int resultSetConcurrency,
                                      int resultSetHoldability) throws SQLException {
        checkResultSetType(resultSetType);
        checkResultSetConcurrency(resultSetType, resultSetConcurrency);
        checkHoldabilitySupport(resultSetHoldability);
    }

    /**
     * Checks whether <code>resultSetType</code> is supported.
     *
     * @param resultSetType param to be checked
     *
     * @throws SQLFeatureNotSupportedException param is not supported
     * @throws SQLNonTransientException        param has invalid value
     */
    private void checkResultSetType(int resultSetType) throws SQLException {
        if (resultSetType != ResultSet.TYPE_FORWARD_ONLY &&
            resultSetType != ResultSet.TYPE_SCROLL_INSENSITIVE &&
            resultSetType != ResultSet.TYPE_SCROLL_SENSITIVE) {
            throw new SQLNonTransientException("", SQLStates.INVALID_PARAMETER_VALUE.getSqlState());
        }
        if (!getMetaData().supportsResultSetType(resultSetType)) {
            throw new SQLFeatureNotSupportedException();
        }
    }

    /**
     * Checks whether <code>resultSetType</code> is supported.
     *
     * @param resultSetConcurrency param to be checked
     *
     * @throws SQLFeatureNotSupportedException param is not supported
     * @throws SQLNonTransientException        param has invalid value
     */
    private void checkResultSetConcurrency(int resultSetType, int resultSetConcurrency) throws SQLException {
        if (resultSetConcurrency != ResultSet.CONCUR_READ_ONLY &&
            resultSetConcurrency != ResultSet.CONCUR_UPDATABLE) {
            throw new SQLNonTransientException("", SQLStates.INVALID_PARAMETER_VALUE.getSqlState());
        }
        if (!getMetaData().supportsResultSetConcurrency(resultSetType, resultSetConcurrency)) {
            throw new SQLFeatureNotSupportedException();
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
        JdbcConstants.checkHoldabilityConstant(holdability);
        if (!getMetaData().supportsResultSetHoldability(holdability)) {
            throw new SQLFeatureNotSupportedException();
        }
    }

    /**
     * Provides error message that contains parameters of failed SQL statement.
     *
     * @param query SQL query
     *
     * @return Formatted error message.
     */
    private static String formatError(SQLQueryHolder query) {
        return "Failed to execute SQL: " + query.getQuery() + ", params: " + query.getParams();
    }

    static class SQLTarantoolClientImpl extends TarantoolClientImpl {

        private Future<?> executeQuery(SQLQueryHolder queryHolder) {
            return exec(Code.EXECUTE, Key.SQL_TEXT, queryHolder.getQuery(), Key.SQL_BIND, queryHolder.getParams());
        }

        private Future<?> executeQuery(SQLQueryHolder queryHolder, long timeoutMillis) {
            return exec(
                timeoutMillis, Code.EXECUTE, Key.SQL_TEXT, queryHolder.getQuery(), Key.SQL_BIND, queryHolder.getParams()
            );
        }

        final SQLRawOps sqlRawOps = new SQLRawOps() {
            @Override
            public SQLResultHolder execute(SQLQueryHolder query) {
                return (SQLResultHolder) syncGet(executeQuery(query));
            }

            @Override
            public SQLResultHolder execute(long timeoutMillis, SQLQueryHolder query) {
                return (SQLResultHolder) syncGet(executeQuery(query, timeoutMillis));
            }

            @Override
            public SQLBatchResultHolder executeBatch(List<SQLQueryHolder> queries) {
                return executeInternal(queries, (query) -> executeQuery(query));
            }

            @Override
            public SQLBatchResultHolder executeBatch(long timeoutMillis, List<SQLQueryHolder> queries) {
                return executeInternal(queries, (query) -> executeQuery(query, timeoutMillis));
            }

            private SQLBatchResultHolder executeInternal(List<SQLQueryHolder> queries,
                                                         Function<SQLQueryHolder, Future<?>> fetcher) {
                List<Future<?>> sqlFutures = new ArrayList<>();
                // using queries pipelining to emulate a batch request
                for (SQLQueryHolder query : queries) {
                    sqlFutures.add(fetcher.apply(query));
                }
                // wait for all the results
                Exception lastError = null;
                List<SQLResultHolder> items = new ArrayList<>(queries.size());
                for (Future<?> future : sqlFutures) {
                    try {
                        SQLResultHolder result = (SQLResultHolder) syncGet(future);
                        if (result.isQueryResult()) {
                            lastError = new SQLException(
                                "Result set is not allowed in the batch response",
                                SQLStates.TOO_MANY_RESULTS.getSqlState()
                            );
                        }
                        items.add(result);
                    } catch (RuntimeException e) {
                        // empty result set will be treated as a wrong result
                        items.add(SQLResultHolder.ofEmptyQuery());
                        lastError = e;
                    }
                }
                return new SQLBatchResultHolder(items, lastError);
            }
        };

        SQLTarantoolClientImpl(String address, TarantoolClientConfig config) {
            super(address, config);
            msgPackLite = SQLMsgPackLite.INSTANCE;
        }

        SQLTarantoolClientImpl(SocketChannelProvider socketProvider, TarantoolClientConfig config) {
            super(socketProvider, config);
            msgPackLite = SQLMsgPackLite.INSTANCE;
        }

        SQLRawOps sqlRawOps() {
            return sqlRawOps;
        }

        @Override
        protected void completeSql(TarantoolOp<?> future, TarantoolPacket pack) {
            Long rowCount = SqlProtoUtils.getSqlRowCount(pack);
            SQLResultHolder result = (rowCount == null)
                ? SQLResultHolder.ofQuery(SqlProtoUtils.getSQLMetadata(pack), SqlProtoUtils.getSQLData(pack))
                : SQLResultHolder.ofUpdate(rowCount.intValue());
            ((TarantoolOp) future).complete(result);
        }

        interface SQLRawOps {

            SQLResultHolder execute(SQLQueryHolder query);

            SQLResultHolder execute(long timeoutMillis, SQLQueryHolder query);

            SQLBatchResultHolder executeBatch(List<SQLQueryHolder> queries);

            SQLBatchResultHolder executeBatch(long timeoutMillis, List<SQLQueryHolder> queries);

        }

    }

}
