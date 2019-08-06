package org.tarantool.jdbc;

import org.tarantool.SqlProtoUtils;
import org.tarantool.util.SQLStates;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.SQLNonTransientException;
import java.util.List;

public class SQLResultSetMetaData implements ResultSetMetaData {

    private final List<SqlProtoUtils.SQLMetaData> sqlMetadata;
    private final boolean readOnly;

    public SQLResultSetMetaData(List<SqlProtoUtils.SQLMetaData> sqlMetaData, boolean readOnly) {
        this.sqlMetadata = sqlMetaData;
        this.readOnly = readOnly;
    }

    @Override
    public int getColumnCount() throws SQLException {
        return sqlMetadata.size();
    }

    @Override
    public boolean isAutoIncrement(int column) throws SQLException {
        checkColumnIndex(column);
        // XXX: extra flag or, at least table ID is required in meta
        // to be able to fetch an the flag indirectly.
        return false;
    }

    @Override
    public boolean isCaseSensitive(int column) throws SQLException {
        checkColumnIndex(column);
        return sqlMetadata.get(column - 1).getType().isCaseSensitive();
    }

    /**
     * {@inheritDoc}
     * <p>
     * All the types can be used in {@literal WHERE} clause.
     */
    @Override
    public boolean isSearchable(int column) throws SQLException {
        checkColumnIndex(column);
        return true;
    }

    /**
     * {@inheritDoc}
     * <p>
     * Always {@literal false} because
     * Tarantool does not have monetary types.
     */
    @Override
    public boolean isCurrency(int column) throws SQLException {
        checkColumnIndex(column);
        return false;
    }

    @Override
    public int isNullable(int column) throws SQLException {
        checkColumnIndex(column);
        // XXX: extra nullability flag or, at least table ID is required in meta
        // to be able to fetch an the flag indirectly.
        return ResultSetMetaData.columnNullableUnknown;
    }

    @Override
    public boolean isSigned(int column) throws SQLException {
        checkColumnIndex(column);
        return sqlMetadata.get(column - 1).getType().isSigned();
    }

    @Override
    public int getColumnDisplaySize(int column) throws SQLException {
        checkColumnIndex(column);
        return sqlMetadata.get(column - 1).getType().getDisplaySize();
    }

    @Override
    public String getColumnLabel(int column) throws SQLException {
        checkColumnIndex(column);
        return sqlMetadata.get(column - 1).getName();
    }

    /**
     * {@inheritDoc}
     * <p>
     * Name always has the same value as label
     * because Tarantool does not differentiate
     * column names and aliases.
     *
     * @see #getColumnLabel(int)
     */
    @Override
    public String getColumnName(int column) throws SQLException {
        checkColumnIndex(column);
        return sqlMetadata.get(column - 1).getName();
    }

    @Override
    public String getSchemaName(int column) throws SQLException {
        checkColumnIndex(column);
        return "";
    }

    @Override
    public int getPrecision(int column) throws SQLException {
        checkColumnIndex(column);
        return sqlMetadata.get(column - 1).getType().getPrecision();
    }


    @Override
    public int getScale(int column) throws SQLException {
        checkColumnIndex(column);
        return sqlMetadata.get(column - 1).getType().getScale();
    }

    @Override
    public String getTableName(int column) throws SQLException {
        checkColumnIndex(column);
        // XXX: extra table name or, at least table ID is required in meta
        // to be able to fetch the table name.
        return "";
    }

    @Override
    public String getCatalogName(int column) throws SQLException {
        checkColumnIndex(column);
        return "";
    }

    @Override
    public int getColumnType(int column) throws SQLException {
        checkColumnIndex(column);
        return sqlMetadata.get(column - 1).getType().getJdbcType().getTypeNumber();
    }

    @Override
    public String getColumnTypeName(int column) throws SQLException {
        checkColumnIndex(column);
        return sqlMetadata.get(column - 1).getType().getTypeName();
    }

    @Override
    public boolean isReadOnly(int column) throws SQLException {
        checkColumnIndex(column);
        return readOnly;
    }

    @Override
    public boolean isWritable(int column) throws SQLException {
        return !isReadOnly(column);
    }

    @Override
    public boolean isDefinitelyWritable(int column) throws SQLException {
        return false;
    }

    @Override
    public String getColumnClassName(int column) throws SQLException {
        checkColumnIndex(column);
        return sqlMetadata.get(column - 1).getType().getJdbcType().getJavaType().getName();
    }

    @Override
    public <T> T unwrap(Class<T> type) throws SQLException {
        if (isWrapperFor(type)) {
            return type.cast(this);
        }
        throw new SQLNonTransientException("SQLResultSetMetadata does not wrap " + type.getName());
    }

    @Override
    public boolean isWrapperFor(Class<?> type) throws SQLException {
        return type.isAssignableFrom(this.getClass());
    }

    void checkColumnIndex(int columnIndex) throws SQLException {
        if (columnIndex < 1 || columnIndex > getColumnCount()) {
            throw new SQLNonTransientException(
                    String.format("Column index %d is out of range. Max index is %d", columnIndex, getColumnCount()),
                    SQLStates.INVALID_PARAMETER_VALUE.getSqlState()
            );
        }
    }

    @Override
    public String toString() {
        return "SQLResultSetMetaData{" +
            "sqlMetadata=" + sqlMetadata +
            '}';
    }
}
