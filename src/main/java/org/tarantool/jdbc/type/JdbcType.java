package org.tarantool.jdbc.type;

import java.sql.Blob;
import java.sql.Clob;
import java.sql.JDBCType;
import java.sql.NClob;

/**
 * Describes supported JDBC types that match
 * Tarantool SQL types.
 *
 * Skipped unsupported types are
 * numeric types {@link JDBCType#NUMERIC}, {@link JDBCType#DECIMAL};
 * date types {@link JDBCType#DATE}, {@link JDBCType#TIME}, {@link JDBCType#TIMESTAMP};
 * xml type {@link JDBCType#SQLXML}.
 */
public enum JdbcType {

    UNKNOWN(Object.class, JDBCType.OTHER),

    CHAR(String.class, JDBCType.CHAR),
    VARCHAR(String.class, JDBCType.VARCHAR),
    LONGVARCHAR(String.class, JDBCType.LONGNVARCHAR),

    NCHAR(String.class, JDBCType.NCHAR),
    NVARCHAR(String.class, JDBCType.NVARCHAR),
    LONGNVARCHAR(String.class, JDBCType.LONGNVARCHAR),

    BINARY(byte[].class, JDBCType.BINARY),
    VARBINARY(byte[].class, JDBCType.VARBINARY),
    LONGVARBINARY(byte[].class, JDBCType.LONGVARBINARY),

    BIT(Boolean.class, JDBCType.BIT),
    BOOLEAN(Boolean.class, JDBCType.BOOLEAN),

    REAL(Float.class, JDBCType.REAL),
    FLOAT(Double.class, JDBCType.FLOAT),
    DOUBLE(Double.class, JDBCType.DOUBLE),

    TINYINT(Byte.class, JDBCType.TINYINT),
    SMALLINT(Short.class, JDBCType.SMALLINT),
    INTEGER(Integer.class, JDBCType.INTEGER),
    BIGINT(Long.class, JDBCType.BIGINT),

    CLOB(Clob.class, JDBCType.CLOB),
    NCLOB(NClob.class, JDBCType.NCLOB),
    BLOB(Blob.class, JDBCType.BLOB);

    private final Class<?> javaType;
    private final JDBCType targetJdbcType;

    JdbcType(Class<?> javaType, JDBCType targetJdbcType) {
        this.javaType = javaType;
        this.targetJdbcType = targetJdbcType;
    }

    public Class<?> getJavaType() {
        return javaType;
    }

    public JDBCType getTargetJdbcType() {
        return targetJdbcType;
    }

    public int getTypeNumber() {
        return targetJdbcType.getVendorTypeNumber();
    }
}
