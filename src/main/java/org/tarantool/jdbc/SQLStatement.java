package org.tarantool.jdbc;

import org.tarantool.JDBCBridge;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLNonTransientException;
import java.sql.SQLWarning;
import java.sql.Statement;

@SuppressWarnings("Since15")
public class SQLStatement implements Statement {

    protected final SQLConnection connection;

    private SQLResultSet resultSet;
    private final int resultSetType;
    private final int resultSetConcurrency;
    private final int resultSetHoldability;

    private int updateCount;
    private int maxRows;

    protected SQLStatement(SQLConnection sqlConnection) throws SQLException {
        this.connection = sqlConnection;
        this.resultSetType = ResultSet.TYPE_FORWARD_ONLY;
        this.resultSetConcurrency = ResultSet.CONCUR_READ_ONLY;
        this.resultSetHoldability = sqlConnection.getHoldability();
    }

    protected SQLStatement(SQLConnection sqlConnection,
                           int resultSetType,
                           int resultSetConcurrency,
                           int resultSetHoldability) throws SQLException {
        this.connection = sqlConnection;
        this.resultSetType = resultSetType;
        this.resultSetConcurrency = resultSetConcurrency;
        this.resultSetHoldability = resultSetHoldability;
    }

    @Override
    public ResultSet executeQuery(String sql) throws SQLException {
        checkNotClosed();
        discardLastResults();
        return createResultSet(connection.executeQuery(sql));
    }

    @Override
    public int executeUpdate(String sql) throws SQLException {
        checkNotClosed();
        discardLastResults();
        return connection.executeUpdate(sql);
    }

    @Override
    public void close() throws SQLException {

    }

    @Override
    public int getMaxFieldSize() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setMaxFieldSize(int max) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public int getMaxRows() throws SQLException {
        checkNotClosed();
        return maxRows;
    }

    @Override
    public void setMaxRows(int maxRows) throws SQLException {
        checkNotClosed();
        if (maxRows < 0) {
            throw new SQLNonTransientException("Max rows parameter can't be a negative value");
        }
        this.maxRows = maxRows;
        if (resultSet != null) {
            resultSet.setMaxRows(this.maxRows);
        }
    }

    @Override
    public void setEscapeProcessing(boolean enable) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public int getQueryTimeout() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setQueryTimeout(int seconds) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void cancel() throws SQLException {

    }

    @Override
    public SQLWarning getWarnings() throws SQLException {
        return null;
    }

    @Override
    public void clearWarnings() throws SQLException {

    }

    @Override
    public void setCursorName(String name) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public boolean execute(String sql) throws SQLException {
        checkNotClosed();
        discardLastResults();
        return handleResult(connection.execute(sql));
    }

    @Override
    public ResultSet getResultSet() throws SQLException {
        checkNotClosed();
        try {
            return resultSet;
        } finally {
            resultSet = null;
        }
    }

    @Override
    public int getUpdateCount() throws SQLException {
        checkNotClosed();
        try {
            return updateCount;
        } finally {
            updateCount = -1;
        }
    }

    @Override
    public boolean getMoreResults() throws SQLException {
        checkNotClosed();
        return false;
    }

    @Override
    public void setFetchDirection(int direction) throws SQLException {
        checkNotClosed();
        if (direction != ResultSet.FETCH_FORWARD) {
            throw new SQLFeatureNotSupportedException();
        }
    }

    @Override
    public int getFetchDirection() throws SQLException {
        checkNotClosed();
        return ResultSet.FETCH_FORWARD;
    }

    @Override
    public void setFetchSize(int rows) throws SQLException {
        checkNotClosed();
        // no-op
    }

    @Override
    public int getFetchSize() throws SQLException {
        return 0;
    }

    @Override
    public int getResultSetConcurrency() throws SQLException {
        checkNotClosed();
        return resultSetConcurrency;
    }

    @Override
    public int getResultSetType() throws SQLException {
        checkNotClosed();
        return resultSetType;
    }

    @Override
    public void addBatch(String sql) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void clearBatch() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public int[] executeBatch() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public Connection getConnection() throws SQLException {
        return connection;
    }

    @Override
    public boolean getMoreResults(int current) throws SQLException {
        checkNotClosed();
        return false;
    }

    @Override
    public ResultSet getGeneratedKeys() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public int executeUpdate(String sql, int autoGeneratedKeys) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public int executeUpdate(String sql, int[] columnIndexes) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public int executeUpdate(String sql, String[] columnNames) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public boolean execute(String sql, int autoGeneratedKeys) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public boolean execute(String sql, int[] columnIndexes) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public boolean execute(String sql, String[] columnNames) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public int getResultSetHoldability() throws SQLException {
        checkNotClosed();
        return resultSetHoldability;
    }

    @Override
    public boolean isClosed() throws SQLException {
        return connection.isClosed();
    }

    @Override
    public void setPoolable(boolean poolable) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public boolean isPoolable() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void closeOnCompletion() throws SQLException {

    }

    @Override
    public boolean isCloseOnCompletion() throws SQLException {
        checkNotClosed();
        return false;
    }

    @Override
    public <T> T unwrap(Class<T> type) throws SQLException {
        if (isWrapperFor(type)) {
            return type.cast(this);
        }
        throw new SQLNonTransientException("Statement does not wrap " + type.getName());
    }

    @Override
    public boolean isWrapperFor(Class<?> type) throws SQLException {
        return type.isAssignableFrom(this.getClass());
    }

    /**
     * Clears the results of the most recent execution.
     */
    protected void discardLastResults() {
        updateCount = -1;
        if (resultSet != null) {
            try {
                resultSet.close();
            } catch (Exception ignored) {
                // No-op.
            }
            resultSet = null;
        }
    }

    /**
     * Sets the internals according to the result of last execution.
     *
     * @param result The result of SQL statement execution.
     * @return {@code true}, if the result is a ResultSet object.
     */
    protected boolean handleResult(Object result) throws SQLException {
        if (result instanceof JDBCBridge) {
            resultSet = createResultSet((JDBCBridge) result);
            resultSet.setMaxRows(maxRows);
            updateCount = -1;
            return true;
        } else {
            resultSet = null;
            updateCount = (Integer) result;
            return false;
        }
    }

    /**
     * Returns {@link ResultSet} which will be initialized by <code>data</code>
     *
     * @param data predefined result to be wrapped by {@link ResultSet}
     * @return wrapped result
     * @throws SQLException if a database access error occurs or
     *                      this method is called on a closed <code>Statement</code>
     */
    public ResultSet executeMetadata(JDBCBridge data) throws SQLException {
        checkNotClosed();
        return createResultSet(data);
    }

    protected SQLResultSet createResultSet(JDBCBridge result) throws SQLException {
        return new SQLResultSet(result, this);
    }

    protected void checkNotClosed() throws SQLException {
        if (isClosed()) {
            throw new SQLNonTransientException("Statement is closed.");
        }
    }
}
