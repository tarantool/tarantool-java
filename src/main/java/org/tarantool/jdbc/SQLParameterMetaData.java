package org.tarantool.jdbc;

import org.tarantool.SqlProtoUtils;
import org.tarantool.util.SQLStates;

import java.sql.ParameterMetaData;
import java.sql.SQLException;
import java.sql.SQLNonTransientException;
import java.util.List;

public class SQLParameterMetaData implements ParameterMetaData {

    private final List<SqlProtoUtils.SQLMetaData> metaData;

    public SQLParameterMetaData(List<SqlProtoUtils.SQLMetaData> metaData) {
        this.metaData = metaData;
    }

    @Override
    public int getParameterCount() {
        return metaData.size();
    }

    @Override
    public int isNullable(int param) throws SQLException {
        checkParameterIndex(param);
        return ParameterMetaData.parameterNullableUnknown;
    }

    @Override
    public boolean isSigned(int param) throws SQLException {
        return getAtIndex(param).getType().isSigned();
    }

    @Override
    public int getPrecision(int param) throws SQLException {
        return getAtIndex(param).getType().getPrecision();
    }

    @Override
    public int getScale(int param) throws SQLException {
        return getAtIndex(param).getType().getScale();
    }

    @Override
    public int getParameterType(int param) throws SQLException {
        return getAtIndex(param).getType().getJdbcType().getTypeNumber();
    }

    @Override
    public String getParameterTypeName(int param) throws SQLException {
        return getAtIndex(param).getType().getTypeName();
    }

    @Override
    public String getParameterClassName(int param) throws SQLException {
        return getAtIndex(param).getType().getJdbcType().getJavaType().getName();
    }

    @Override
    public int getParameterMode(int param) throws SQLException {
        checkParameterIndex(param);
        return ParameterMetaData.parameterModeIn;
    }

    @Override
    public <T> T unwrap(Class<T> type) throws SQLException {
        if (isWrapperFor(type)) {
            return type.cast(this);
        }
        throw new SQLNonTransientException("SQLParameterMetaData does not wrap " + type.getName());
    }

    @Override
    public boolean isWrapperFor(Class<?> type) throws SQLException {
        return type.isAssignableFrom(this.getClass());
    }

    private SqlProtoUtils.SQLMetaData getAtIndex(int index) throws SQLException {
        checkParameterIndex(index);
        return metaData.get(index - 1);
    }

    private void checkParameterIndex(int index) throws SQLException {
        int parameterCount = getParameterCount();
        if (index < 1 || index > parameterCount) {
            throw new SQLNonTransientException(
                String.format("Parameter index %d is out of range. Max index is %d", index, parameterCount),
                SQLStates.INVALID_PARAMETER_VALUE.getSqlState()
            );
        }
    }
}
