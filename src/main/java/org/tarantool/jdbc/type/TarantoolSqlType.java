package org.tarantool.jdbc.type;

import java.util.HashMap;
import java.util.Map;

/**
 * Describes supported SQL types by Tarantool DB.
 */
public enum TarantoolSqlType {

    UNKNOWN(TarantoolType.UNKNOWN, JdbcType.UNKNOWN, "unknown"),

    // float, double, real used to be number aliases before 2.2
    FLOAT(TarantoolType.NUMBER, JdbcType.FLOAT, "float"),
    DOUBLE(TarantoolType.NUMBER, JdbcType.DOUBLE, "double"),
    REAL(TarantoolType.NUMBER, JdbcType.REAL, "real"),
    // was introduced in Tarantool 2.2.1
    NUMBER(TarantoolType.NUMBER, JdbcType.DOUBLE, "number"),

    INT(TarantoolType.INTEGER, JdbcType.BIGINT, "int"),
    INTEGER(TarantoolType.INTEGER, JdbcType.BIGINT, "integer"),
    // was introduced in 2.2
    UNSIGNED(TarantoolType.UNSIGNED, JdbcType.BIGINT, "integer"),

    // were introduced in 2.2
    BOOL(TarantoolType.BOOLEAN, JdbcType.BOOLEAN, "bool"),
    BOOLEAN(TarantoolType.BOOLEAN, JdbcType.BOOLEAN, "boolean"),

    STRING(TarantoolType.STRING, JdbcType.VARCHAR, "string"),
    TEXT(TarantoolType.STRING, JdbcType.VARCHAR, "text"),
    VARCHAR(TarantoolType.STRING, JdbcType.VARCHAR, "varchar") {
        @Override
        public String getDisplayType() {
            return getTypeName() + "(128)";
        }
    },

    // was introduced in 2.2
    VARBINARY(TarantoolType.VARBINARY, JdbcType.VARBINARY, "varbinary"),

    SCALAR(TarantoolType.SCALAR, JdbcType.BINARY, "scalar");

    private static final Map<TarantoolType, TarantoolSqlType> defaultSqlTypeMapping;
    static {
        defaultSqlTypeMapping = new HashMap<>();
        defaultSqlTypeMapping.put(TarantoolType.BOOLEAN, TarantoolSqlType.BOOLEAN);
        defaultSqlTypeMapping.put(TarantoolType.STRING, TarantoolSqlType.STRING);
        defaultSqlTypeMapping.put(TarantoolType.INTEGER, TarantoolSqlType.INTEGER);
        defaultSqlTypeMapping.put(TarantoolType.UNSIGNED, TarantoolSqlType.UNSIGNED);
        defaultSqlTypeMapping.put(TarantoolType.NUMBER, TarantoolSqlType.NUMBER);
        defaultSqlTypeMapping.put(TarantoolType.VARBINARY, TarantoolSqlType.VARBINARY);
        defaultSqlTypeMapping.put(TarantoolType.SCALAR, TarantoolSqlType.SCALAR);
    }

    public static TarantoolSqlType getDefaultSqlType(TarantoolType type) {
        return defaultSqlTypeMapping.getOrDefault(type, TarantoolSqlType.UNKNOWN);
    }

    /**
     * Corresponding raw {@link TarantoolType}.
     */
    private final TarantoolType tarantoolType;

    /**
     * Corresponding {@link JdbcType}.
     */
    private final JdbcType jdbcType;

    /**
     * Name of Tarantool SQL type.
     */
    private final String typeName;

    public static TarantoolSqlType of(String type) {
        for (TarantoolSqlType value : TarantoolSqlType.values()) {
            if (value.typeName.equalsIgnoreCase(type)) {
                return value;
            }
        }
        return UNKNOWN;
    }

    TarantoolSqlType(TarantoolType tarantoolType, JdbcType jdbcType, String typeName) {
        this.tarantoolType = tarantoolType;
        this.jdbcType = jdbcType;
        this.typeName = typeName;
    }

    public String getTypeName() {
        return typeName;
    }

    public String getDisplayType() {
        return typeName;
    }

    public TarantoolType getTarantoolType() {
        return tarantoolType;
    }

    public JdbcType getJdbcType() {
        return jdbcType;
    }

    public boolean isSigned() {
        return tarantoolType.isSigned();
    }

    public boolean isCaseSensitive() {
        return tarantoolType.isCaseSensitive();
    }

    public int getPrecision() {
        return tarantoolType.getPrecision();
    }

    public int getScale() {
        return tarantoolType.getScale();
    }

    public int getDisplaySize() {
        return tarantoolType.getDisplaySize();
    }

    @Override
    public String toString() {
        return "TarantoolSqlType{" +
            "tarantoolType=" + tarantoolType +
            ", jdbcType=" + jdbcType +
            ", typeName='" + typeName + '\'' +
            '}';
    }
}
