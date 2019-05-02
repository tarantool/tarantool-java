package org.tarantool.jdbc;

import org.tarantool.util.SQLStates;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.NClob;
import java.sql.ParameterMetaData;
import java.sql.PreparedStatement;
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLXML;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Map;

public class SQLPreparedStatement extends SQLStatement implements PreparedStatement {

    static final String INVALID_CALL_MSG = "The method cannot be called on a PreparedStatement.";
    final String sql;
    final Map<Integer, Object> params;


    public SQLPreparedStatement(SQLConnection connection, String sql) throws SQLException {
        super(connection);
        this.sql = sql;
        this.params = new HashMap<>();
    }

    public SQLPreparedStatement(SQLConnection connection,
                                String sql,
                                int resultSetType,
                                int resultSetConcurrency,
                                int resultSetHoldability) throws SQLException {
        super(connection, resultSetType, resultSetConcurrency, resultSetHoldability);
        this.sql = sql;
        this.params = new HashMap<>();
    }

    @Override
    public ResultSet executeQuery() throws SQLException {
        checkNotClosed();
        if (!executeInternal(sql, getParams())) {
            throw new SQLException("No results were returned", SQLStates.NO_DATA.getSqlState());
        }
        return resultSet;
    }

    @Override
    public ResultSet executeQuery(String sql) throws SQLException {
        throw new SQLException(INVALID_CALL_MSG);
    }

    protected Object[] getParams() throws SQLException {
        Object[] objects = new Object[params.size()];
        for (int i = 1; i <= params.size(); i++) {
            if (params.containsKey(i)) {
                objects[i - 1] = params.get(i);
            } else {
                throw new SQLException("Parameter " + i + " is missing");
            }
        }
        return objects;
    }

    @Override
    public int executeUpdate() throws SQLException {
        checkNotClosed();
        if (executeInternal(sql, getParams())) {
            throw new SQLException(
                "Result was returned but nothing was expected",
                SQLStates.TOO_MANY_RESULTS.getSqlState()
            );
        }
        return updateCount;
    }

    @Override
    public int executeUpdate(String sql) throws SQLException {
        throw new SQLException(INVALID_CALL_MSG);
    }

    @Override
    public void setNull(int parameterIndex, int sqlType) throws SQLException {
        setParameter(parameterIndex, null);
    }

    @Override
    public void setNull(int parameterIndex, int sqlType, String typeName) throws SQLException {
        setParameter(parameterIndex, null);
    }

    @Override
    public void setBoolean(int parameterIndex, boolean parameterValue) throws SQLException {
        setParameter(parameterIndex, parameterValue);
    }

    @Override
    public void setByte(int parameterIndex, byte parameterValue) throws SQLException {
        setParameter(parameterIndex, parameterValue);
    }

    @Override
    public void setBytes(int parameterIndex, byte[] parameterValue) throws SQLException {
        setParameter(parameterIndex, parameterValue);
    }

    @Override
    public void setShort(int parameterIndex, short parameterValue) throws SQLException {
        setParameter(parameterIndex, parameterValue);
    }

    @Override
    public void setInt(int parameterIndex, int parameterValue) throws SQLException {
        setParameter(parameterIndex, parameterValue);
    }

    @Override
    public void setLong(int parameterIndex, long parameterValue) throws SQLException {
        setParameter(parameterIndex, parameterValue);
    }

    @Override
    public void setFloat(int parameterIndex, float parameterValue) throws SQLException {
        setParameter(parameterIndex, parameterValue);
    }

    @Override
    public void setDouble(int parameterIndex, double parameterValue) throws SQLException {
        setParameter(parameterIndex, parameterValue);
    }

    @Override
    public void setBigDecimal(int parameterIndex, BigDecimal parameterValue) throws SQLException {
        setParameter(parameterIndex, parameterValue);
    }

    @Override
    public void setString(int parameterIndex, String parameterValue) throws SQLException {
        setParameter(parameterIndex, parameterValue);
    }

    @Override
    public void setDate(int parameterIndex, Date parameterValue) throws SQLException {
        setParameter(parameterIndex, parameterValue);
    }

    @Override
    public void setDate(int parameterIndex, Date parameterValue, Calendar calendar) throws SQLException {
        setParameter(parameterIndex, parameterValue);
    }

    @Override
    public void setTime(int parameterIndex, Time parameterValue) throws SQLException {
        setParameter(parameterIndex, parameterValue);
    }

    @Override
    public void setTime(int parameterIndex, Time parameterValue, Calendar calendar) throws SQLException {
        setParameter(parameterIndex, parameterValue);
    }

    @Override
    public void setTimestamp(int parameterIndex, Timestamp parameterValue) throws SQLException {
        setParameter(parameterIndex, parameterValue);
    }

    @Override
    public void setTimestamp(int parameterIndex, Timestamp parameterValue, Calendar calendar) throws SQLException {
        setParameter(parameterIndex, parameterValue);
    }

    @Override
    public void setAsciiStream(int parameterIndex, InputStream parameterValue, int length) throws SQLException {
        setParameter(parameterIndex, parameterValue);
    }

    @Override
    public void setAsciiStream(int parameterIndex, InputStream x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setAsciiStream(int parameterIndex, InputStream x, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setUnicodeStream(int parameterIndex, InputStream parameterValue, int length) throws SQLException {
        setParameter(parameterIndex, parameterValue);
    }

    @Override
    public void setBinaryStream(int parameterIndex, InputStream parameterValue, int length) throws SQLException {
        setParameter(parameterIndex, parameterValue);
    }

    @Override
    public void setBinaryStream(int parameterIndex, InputStream x, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setBinaryStream(int parameterIndex, InputStream x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void clearParameters() throws SQLException {
        params.clear();
    }

    @Override
    public void setObject(int parameterIndex, Object x, int targetSqlType) throws SQLException {
        setObject(parameterIndex, x, targetSqlType, -1);
    }

    @Override
    public void setObject(int parameterIndex, Object value) throws SQLException {
        setParameter(parameterIndex, value);
    }

    @Override
    public void setObject(int parameterIndex,
                          Object parameterValue,
                          int targetSqlType,
                          int scaleOrLength) throws SQLException {
        setParameter(parameterIndex, parameterValue);
    }

    private void setParameter(int parameterIndex, Object value) throws SQLException {
        checkNotClosed();
        params.put(parameterIndex, value);
    }

    @Override
    public boolean execute() throws SQLException {
        checkNotClosed();
        return executeInternal(sql, getParams());
    }

    @Override
    public boolean execute(String sql) throws SQLException {
        throw new SQLException(INVALID_CALL_MSG);
    }

    @Override
    public void setCharacterStream(int parameterIndex, Reader reader, int length) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setCharacterStream(int parameterIndex, Reader reader, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setCharacterStream(int parameterIndex, Reader reader) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setRef(int parameterIndex, Ref x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setBlob(int parameterIndex, Blob x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setBlob(int parameterIndex, InputStream inputStream, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setBlob(int parameterIndex, InputStream inputStream) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setClob(int parameterIndex, Clob x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setClob(int parameterIndex, Reader reader, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setClob(int parameterIndex, Reader reader) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setArray(int parameterIndex, Array x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public ResultSetMetaData getMetaData() throws SQLException {
        return getResultSet().getMetaData();
    }

    @Override
    public void setURL(int parameterIndex, URL parameterValue) throws SQLException {
        setParameter(parameterIndex, parameterValue.toString());
    }

    @Override
    public ParameterMetaData getParameterMetaData() throws SQLException {
        return null;
    }

    @Override
    public void setRowId(int parameterIndex, RowId x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setNString(int parameterIndex, String parameterValue) throws SQLException {
        setParameter(parameterIndex, parameterValue);
    }

    @Override
    public void setNCharacterStream(int parameterIndex, Reader value, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setNCharacterStream(int parameterIndex, Reader value) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setNClob(int parameterIndex, NClob value) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setNClob(int parameterIndex, Reader reader, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setNClob(int parameterIndex, Reader reader) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setSQLXML(int parameterIndex, SQLXML xmlObject) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void addBatch(String sql) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void addBatch() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public int[] executeBatch() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void clearBatch() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

}
