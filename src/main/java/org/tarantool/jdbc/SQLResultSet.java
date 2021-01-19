package org.tarantool.jdbc;

import org.tarantool.jdbc.cursor.CursorIterator;
import org.tarantool.jdbc.cursor.InMemoryForwardCursorIteratorImpl;
import org.tarantool.jdbc.cursor.InMemoryScrollableCursorIteratorImpl;
import org.tarantool.util.SQLStates;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.Reader;
import java.io.StringReader;
import java.math.BigDecimal;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.NClob;
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLNonTransientException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

public class SQLResultSet implements ResultSet {

    private CursorIterator<List<Object>> iterator;
    private TarantoolResultSetMetaData metaData;

    private Map<String, Integer> columnByNameLookups;
    private boolean lastColumnWasNull;

    private final TarantoolStatement statement;
    private final int maxRows;
    private final int maxFieldSize;

    private final AtomicBoolean isClosed = new AtomicBoolean(false);

    private final int scrollType;
    private final int concurrencyLevel;
    private final int holdability;

    public SQLResultSet(SQLResultHolder holder, TarantoolStatement ownerStatement) throws SQLException {
        metaData = new SQLResultSetMetaData(holder.getSqlMetadata(), ownerStatement.getConnection().isReadOnly());
        statement = ownerStatement;
        scrollType = statement.getResultSetType();
        concurrencyLevel = statement.getResultSetConcurrency();
        holdability = statement.getResultSetHoldability();
        maxRows = statement.getMaxRows();
        maxFieldSize = statement.getMaxFieldSize();

        List<List<Object>> fetchedRows = holder.getRows();
        List<List<Object>> rows = maxRows == 0 || maxRows >= fetchedRows.size()
            ? fetchedRows
            : fetchedRows.subList(0, maxRows);

        switch (scrollType) {
        case ResultSet.TYPE_FORWARD_ONLY:
            iterator = new InMemoryForwardCursorIteratorImpl(rows);
            break;
        case ResultSet.TYPE_SCROLL_INSENSITIVE:
            iterator = new InMemoryScrollableCursorIteratorImpl(rows);
            break;
        default:
            throw new SQLNonTransientException("", SQLStates.INVALID_PARAMETER_VALUE.getSqlState());
        }
    }

    public int getMaxRows() {
        return maxRows;
    }

    public List<Object> getCurrentRow() throws SQLException {
        checkNotClosed();
        return iterator.getItem();
    }

    protected Object getRaw(int columnIndex) throws SQLException {
        checkNotClosed();
        metaData.checkColumnIndex(columnIndex);
        List<Object> row = getCurrentRow();
        Object value = row.get(columnIndex - 1);
        lastColumnWasNull = (value == null);
        return value;
    }

    protected Number getNumber(int columnIndex) throws SQLException {
        Number raw = (Number) getRaw(columnIndex);
        return raw == null ? 0 : raw;
    }

    protected Number getNullableNumber(int columnIndex) throws SQLException {
        return (Number) getRaw(columnIndex);
    }

    @Override
    public void close() throws SQLException {
        if (isClosed.compareAndSet(false, true)) {
            try {
                iterator.close();
                iterator = null;
                metaData = null;
            } finally {
                statement.checkCompletion();
            }
        }
    }

    @Override
    public boolean wasNull() throws SQLException {
        checkNotClosed();
        return lastColumnWasNull;
    }

    @Override
    public String getString(int columnIndex) throws SQLException {
        Object raw = getRaw(columnIndex);
        if (raw == null) {
            return null;
        }
        String value = String.valueOf(raw);
        return (maxFieldSize > 0 && value.length() > maxFieldSize && metaData.isTrimmable(columnIndex))
            ? value.substring(0, maxFieldSize)
            : value;
    }

    @Override
    public String getString(String columnLabel) throws SQLException {
        return getString(findColumn(columnLabel));
    }

    @Override
    public boolean getBoolean(int columnIndex) throws SQLException {
        return Boolean.TRUE.equals(getRaw(columnIndex));
    }

    @Override
    public boolean getBoolean(String columnLabel) throws SQLException {
        return getBoolean(findColumn(columnLabel));
    }

    @Override
    public byte getByte(int columnIndex) throws SQLException {
        return (getNumber(columnIndex)).byteValue();
    }

    @Override
    public byte getByte(String columnLabel) throws SQLException {
        return getByte(findColumn(columnLabel));
    }

    @Override
    public short getShort(int columnIndex) throws SQLException {
        return (getNumber(columnIndex)).shortValue();
    }

    @Override
    public short getShort(String columnLabel) throws SQLException {
        return getShort(findColumn(columnLabel));
    }

    @Override
    public int getInt(int columnIndex) throws SQLException {
        return getNumber(columnIndex).intValue();
    }

    @Override
    public int getInt(String columnLabel) throws SQLException {
        return getInt(findColumn(columnLabel));
    }

    @Override
    public long getLong(int columnIndex) throws SQLException {
        return (getNumber(columnIndex)).longValue();
    }

    @Override
    public long getLong(String columnLabel) throws SQLException {
        return getLong(findColumn(columnLabel));
    }

    @Override
    public float getFloat(int columnIndex) throws SQLException {
        return (getNumber(columnIndex)).floatValue();
    }

    @Override
    public float getFloat(String columnLabel) throws SQLException {
        return getFloat(findColumn(columnLabel));
    }

    @Override
    public double getDouble(int columnIndex) throws SQLException {
        return (getNumber(columnIndex)).doubleValue();
    }

    @Override
    public double getDouble(String columnLabel) throws SQLException {
        return getDouble(findColumn(columnLabel));
    }

    @Override
    @Deprecated
    public BigDecimal getBigDecimal(int columnIndex, int scale) throws SQLException {
        String raw = getString(columnIndex);
        if (raw == null) {
            return null;
        }
        BigDecimal bigDecimal = new BigDecimal(raw);
        return scale > -1 ? bigDecimal.setScale(scale) : bigDecimal;
    }

    @Override
    @Deprecated
    public BigDecimal getBigDecimal(String columnLabel, int scale) throws SQLException {
        return getBigDecimal(findColumn(columnLabel), scale);
    }

    @Override
    public BigDecimal getBigDecimal(int columnIndex) throws SQLException {
        return getBigDecimal(columnIndex, -1);
    }

    @Override
    public BigDecimal getBigDecimal(String columnLabel) throws SQLException {
        return getBigDecimal(columnLabel, -1);
    }

    @Override
    public byte[] getBytes(int columnIndex) throws SQLException {
        Object raw = getRaw(columnIndex);
        if (raw == null) {
            return null;
        }
        byte[] bytes = (byte[]) raw;
        if (maxFieldSize > 0 && bytes.length > maxFieldSize && metaData.isTrimmable(columnIndex)) {
            byte[] trimmedBytes = new byte[maxFieldSize];
            System.arraycopy(bytes, 0, trimmedBytes, 0, maxFieldSize);
            return trimmedBytes;
        }
        return bytes;
    }

    @Override
    public byte[] getBytes(String columnLabel) throws SQLException {
        return getBytes(findColumn(columnLabel));
    }

    @Override
    public Date getDate(int columnIndex) throws SQLException {
        Number time = getNullableNumber(columnIndex);
        return time == null ? null : new java.sql.Date(time.longValue());
    }

    @Override
    public Date getDate(String columnLabel) throws SQLException {
        return getDate(findColumn(columnLabel));
    }

    @Override
    public Date getDate(int columnIndex, Calendar cal) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public Date getDate(String columnLabel, Calendar cal) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public Time getTime(int columnIndex) throws SQLException {
        Number time = getNullableNumber(columnIndex);
        return time == null ? null : new java.sql.Time(time.longValue());
    }

    @Override
    public Time getTime(String columnLabel) throws SQLException {
        return getTime(findColumn(columnLabel));
    }

    @Override
    public Time getTime(int columnIndex, Calendar cal) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public Time getTime(String columnLabel, Calendar cal) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public Timestamp getTimestamp(int columnIndex) throws SQLException {
        Number time = getNullableNumber(columnIndex);
        return time == null ? null : new java.sql.Timestamp(time.longValue());
    }

    @Override
    public Timestamp getTimestamp(String columnLabel) throws SQLException {
        return getTimestamp(findColumn(columnLabel));
    }

    @Override
    public Timestamp getTimestamp(int columnIndex, Calendar cal) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public Timestamp getTimestamp(String columnLabel, Calendar cal) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }


    @Override
    public InputStream getAsciiStream(int columnIndex) throws SQLException {
        String string = getString(columnIndex);
        return string == null ? null : new ByteArrayInputStream(string.getBytes(StandardCharsets.US_ASCII));
    }

    @Override
    public InputStream getAsciiStream(String columnLabel) throws SQLException {
        return getAsciiStream(findColumn(columnLabel));
    }

    @Override
    @Deprecated
    public InputStream getUnicodeStream(int columnIndex) throws SQLException {
        String string = getString(columnIndex);
        return string == null ? null : new ByteArrayInputStream(string.getBytes(StandardCharsets.UTF_8));
    }

    @Override
    @Deprecated
    public InputStream getUnicodeStream(String columnLabel) throws SQLException {
        return getUnicodeStream(findColumn(columnLabel));
    }

    @Override
    public InputStream getBinaryStream(int columnIndex) throws SQLException {
        byte[] bytes = getBytes(columnIndex);
        return bytes == null ? null : new ByteArrayInputStream(bytes);
    }

    @Override
    public InputStream getBinaryStream(String columnLabel) throws SQLException {
        return getBinaryStream(findColumn(columnLabel));
    }

    @Override
    public Reader getCharacterStream(int columnIndex) throws SQLException {
        String value = getString(columnIndex);
        return value == null ? null : new StringReader(value);
    }

    @Override
    public Reader getCharacterStream(String columnLabel) throws SQLException {
        return getCharacterStream(findColumn(columnLabel));
    }

    @Override
    public Object getObject(int columnIndex) throws SQLException {
        return getRaw(columnIndex);
    }

    @Override
    public Object getObject(String columnLabel) throws SQLException {
        return getObject(findColumn(columnLabel));
    }

    @Override
    public Object getObject(int columnIndex, Map<String, Class<?>> map) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public Object getObject(String columnLabel, Map<String, Class<?>> map) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public <T> T getObject(int columnIndex, Class<T> type) throws SQLException {
        try {
            return type.cast(getRaw(columnIndex));
        } catch (Exception e) {
            throw new SQLNonTransientException(e);
        }
    }

    @Override
    public <T> T getObject(String columnLabel, Class<T> type) throws SQLException {
        return getObject(findColumn(columnLabel), type);
    }

    @Override
    public SQLWarning getWarnings() {
        return null;
    }

    @Override
    public void clearWarnings() {

    }

    @Override
    public String getCursorName() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public ResultSetMetaData getMetaData() throws SQLException {
        checkNotClosed();
        return metaData;
    }

    @Override
    public int findColumn(String columnLabel) throws SQLException {
        checkNotClosed();
        return findColumnIndex(columnLabel);
    }

    protected int findColumnIndex(String columnLabel) throws SQLException {
        if (columnByNameLookups == null) {
            columnByNameLookups = new LinkedHashMap<>();
            // Spec quote: Column labels supplied to getter methods are case insensitive.
            // If a select list contains the same column more than once, the first instance
            // of the column will be returned.
            for (int i = metaData.getColumnCount(); i > 0; i--) {
                columnByNameLookups.put(metaData.getColumnLabel(i).toUpperCase(), i);
            }
        }
        return columnByNameLookups.getOrDefault(columnLabel.toUpperCase(), 0);
    }

    //region Cursor movement API

    @Override
    public boolean next() throws SQLException {
        checkNotClosed();
        return iterator.next();
    }

    @Override
    public boolean isBeforeFirst() throws SQLException {
        checkNotClosed();
        return iterator.isBeforeFirst();
    }

    @Override
    public boolean isAfterLast() throws SQLException {
        checkNotClosed();
        return iterator.isAfterLast();
    }

    @Override
    public boolean isFirst() throws SQLException {
        checkNotClosed();
        return iterator.isFirst();
    }

    @Override
    public boolean isLast() throws SQLException {
        checkNotClosed();
        return iterator.isLast();
    }

    @Override
    public void beforeFirst() throws SQLException {
        checkNotClosed();
        iterator.beforeFirst();
    }

    @Override
    public void afterLast() throws SQLException {
        checkNotClosed();
        iterator.afterLast();
    }

    @Override
    public boolean first() throws SQLException {
        checkNotClosed();
        return iterator.first();
    }

    @Override
    public boolean last() throws SQLException {
        checkNotClosed();
        return iterator.last();
    }

    @Override
    public boolean absolute(int row) throws SQLException {
        checkNotClosed();
        return iterator.absolute(row);
    }

    @Override
    public boolean relative(int rows) throws SQLException {
        checkNotClosed();
        return iterator.relative(rows);
    }

    @Override
    public boolean previous() throws SQLException {
        checkNotClosed();
        return iterator.previous();
    }

    @Override
    public int getRow() throws SQLException {
        checkNotClosed();
        return iterator.getRow();
    }

    //endregion

    @Override
    public void setFetchDirection(int direction) throws SQLException {
        checkNotClosed();
        if (direction != ResultSet.FETCH_FORWARD) {
            throw new SQLException("TYPE_FORWARD_ONLY");
        }
    }

    @Override
    public int getFetchDirection() throws SQLException {
        checkNotClosed();
        return ResultSet.FETCH_FORWARD;
    }

    @Override
    public void setFetchSize(int rows) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public int getFetchSize() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public int getType() throws SQLException {
        checkNotClosed();
        return scrollType;
    }

    @Override
    public int getConcurrency() throws SQLException {
        checkNotClosed();
        return concurrencyLevel;
    }

    @Override
    public boolean rowUpdated() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public boolean rowInserted() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public boolean rowDeleted() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateNull(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateNull(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateBoolean(int columnIndex, boolean x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateBoolean(String columnLabel, boolean x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateByte(int columnIndex, byte x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateByte(String columnLabel, byte x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateShort(int columnIndex, short x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateShort(String columnLabel, short x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateInt(int columnIndex, int x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateInt(String columnLabel, int x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateLong(int columnIndex, long x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateLong(String columnLabel, long x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateFloat(int columnIndex, float x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateFloat(String columnLabel, float x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateDouble(int columnIndex, double x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateDouble(String columnLabel, double x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateBigDecimal(int columnIndex, BigDecimal x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateBigDecimal(String columnLabel, BigDecimal x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateString(int columnIndex, String x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateString(String columnLabel, String x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateBytes(int columnIndex, byte[] x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateBytes(String columnLabel, byte[] x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateDate(int columnIndex, Date x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateDate(String columnLabel, Date x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateTime(int columnIndex, Time x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateTime(String columnLabel, Time x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateTimestamp(int columnIndex, Timestamp x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateTimestamp(String columnLabel, Timestamp x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateAsciiStream(int columnIndex, InputStream x, int length) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateAsciiStream(String columnLabel, InputStream x, int length) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateAsciiStream(int columnIndex, InputStream x, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateAsciiStream(String columnLabel, InputStream x, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateAsciiStream(int columnIndex, InputStream x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateAsciiStream(String columnLabel, InputStream x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateBinaryStream(int columnIndex, InputStream x, int length) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateBinaryStream(String columnLabel, InputStream x, int length) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateBinaryStream(int columnIndex, InputStream x, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateBinaryStream(String columnLabel, InputStream x, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateBinaryStream(int columnIndex, InputStream x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateBinaryStream(String columnLabel, InputStream x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateCharacterStream(int columnIndex, Reader x, int length) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateCharacterStream(String columnLabel, Reader reader, int length) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateCharacterStream(int columnIndex, Reader x, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateCharacterStream(String columnLabel, Reader reader, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateCharacterStream(int columnIndex, Reader x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateCharacterStream(String columnLabel, Reader reader) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateObject(int columnIndex, Object x, int scaleOrLength) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateObject(String columnLabel, Object x, int scaleOrLength) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateObject(int columnIndex, Object x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateObject(String columnLabel, Object x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void insertRow() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateRow() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void deleteRow() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void refreshRow() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void cancelRowUpdates() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void moveToInsertRow() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void moveToCurrentRow() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public Statement getStatement() throws SQLException {
        checkNotClosed();
        return statement;
    }

    @Override
    public Ref getRef(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public Ref getRef(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public Blob getBlob(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public Blob getBlob(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public Clob getClob(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public Clob getClob(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public Array getArray(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public Array getArray(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public URL getURL(int columnIndex) throws SQLException {
        try {
            return new URL(getString(columnIndex));
        } catch (MalformedURLException e) {
            throw new SQLException(e);
        }
    }

    @Override
    public URL getURL(String columnLabel) throws SQLException {
        try {
            return new URL(getString(columnLabel));
        } catch (MalformedURLException e) {
            throw new SQLException(e);
        }
    }

    @Override
    public void updateRef(int columnIndex, Ref x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateRef(String columnLabel, Ref x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateBlob(int columnIndex, Blob x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateBlob(String columnLabel, Blob x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateBlob(int columnIndex, InputStream inputStream, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateBlob(String columnLabel, InputStream inputStream, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateBlob(int columnIndex, InputStream inputStream) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateBlob(String columnLabel, InputStream inputStream) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateClob(int columnIndex, Clob x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateClob(String columnLabel, Clob x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateClob(int columnIndex, Reader reader, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateClob(String columnLabel, Reader reader, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateClob(int columnIndex, Reader reader) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateClob(String columnLabel, Reader reader) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateArray(int columnIndex, Array x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateArray(String columnLabel, Array x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public RowId getRowId(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public RowId getRowId(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateRowId(int columnIndex, RowId x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateRowId(String columnLabel, RowId x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public int getHoldability() throws SQLException {
        checkNotClosed();
        return holdability;
    }

    @Override
    public boolean isClosed() throws SQLException {
        return isClosed.get() || statement.isClosed();
    }

    @Override
    public void updateNString(int columnIndex, String string) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateNString(String columnLabel, String string) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateNClob(int columnIndex, NClob clob) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateNClob(String columnLabel, NClob clob) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateNClob(int columnIndex, Reader reader, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateNClob(String columnLabel, Reader reader, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateNClob(int columnIndex, Reader reader) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateNClob(String columnLabel, Reader reader) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public NClob getNClob(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public NClob getNClob(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public SQLXML getSQLXML(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public SQLXML getSQLXML(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateSQLXML(int columnIndex, SQLXML xmlObject) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateSQLXML(String columnLabel, SQLXML xmlObject) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public String getNString(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public String getNString(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public Reader getNCharacterStream(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public Reader getNCharacterStream(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateNCharacterStream(int columnIndex, Reader x, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateNCharacterStream(String columnLabel, Reader reader, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateNCharacterStream(int columnIndex, Reader x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateNCharacterStream(String columnLabel, Reader reader) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public <T> T unwrap(Class<T> type) throws SQLException {
        if (isWrapperFor(type)) {
            return type.cast(this);
        }
        throw new SQLNonTransientException("SQLResultSet does not wrap " + type.getName());
    }

    @Override
    public boolean isWrapperFor(Class<?> type) throws SQLException {
        return type.isAssignableFrom(this.getClass());
    }

    @Override
    public String toString() {
        return "SQLResultSet{" +
            "metaData=" + metaData +
            ", statement=" + statement +
            ", scrollType=" + scrollType +
            ", concurrencyLevel=" + concurrencyLevel +
            ", holdability=" + holdability +
            '}';
    }

    protected void checkNotClosed() throws SQLException {
        if (isClosed()) {
            throw new SQLNonTransientException("ResultSet is closed.");
        }
    }

}
