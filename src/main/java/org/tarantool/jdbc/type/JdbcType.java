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

    UNKNOWN(Object.class, JDBCType.OTHER, false),

    CHAR(String.class, JDBCType.CHAR, true),
    VARCHAR(String.class, JDBCType.VARCHAR, true),
    LONGVARCHAR(String.class, JDBCType.LONGNVARCHAR, true),

    NCHAR(String.class, JDBCType.NCHAR, true),
    NVARCHAR(String.class, JDBCType.NVARCHAR, true),
    LONGNVARCHAR(String.class, JDBCType.LONGNVARCHAR, true),

    BINARY(byte[].class, JDBCType.BINARY, true),
    VARBINARY(byte[].class, JDBCType.VARBINARY, true),
    LONGVARBINARY(byte[].class, JDBCType.LONGVARBINARY, true),

    BIT(Boolean.class, JDBCType.BIT, false),
    BOOLEAN(Boolean.class, JDBCType.BOOLEAN, false),

    REAL(Float.class, JDBCType.REAL, false),
    FLOAT(Double.class, JDBCType.FLOAT, false),
    DOUBLE(Double.class, JDBCType.DOUBLE, false),

    TINYINT(Byte.class, JDBCType.TINYINT, false),
    SMALLINT(Short.class, JDBCType.SMALLINT, false),
    INTEGER(Integer.class, JDBCType.INTEGER, false),
    BIGINT(Long.class, JDBCType.BIGINT, false),

    CLOB(Clob.class, JDBCType.CLOB, false),
    NCLOB(NClob.class, JDBCType.NCLOB, false),
    BLOB(Blob.class, JDBCType.BLOB, false);

    private final Class<?> javaType;
    private final JDBCType targetJdbcType;
    private final boolean trimmable;

    JdbcType(Class<?> javaType, JDBCType targetJdbcType, boolean trimmable) {
        this.javaType = javaType;
        this.targetJdbcType = targetJdbcType;
        this.trimmable = trimmable;
    }

    public Class<?> getJavaType() {
        return javaType;
    }

    public JDBCType getTargetJdbcType() {
        return targetJdbcType;
    }

    public boolean isTrimmable() {
        return trimmable;
    }

    public int getTypeNumber() {
        return targetJdbcType.getVendorTypeNumber();
    }
}
