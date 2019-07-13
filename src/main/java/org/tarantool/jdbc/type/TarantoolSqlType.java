package org.tarantool.jdbc.type;

import java.util.HashMap;
import java.util.Map;

/**
 * Describes supported SQL types by Tarantool DB.
 */
public enum TarantoolSqlType {

    UNKNOWN(TarantoolType.UNKNOWN, JdbcType.UNKNOWN, "unknown"),

    FLOAT(TarantoolType.NUMBER, JdbcType.FLOAT, "float"),
    DOUBLE(TarantoolType.NUMBER, JdbcType.DOUBLE, "double"),
    REAL(TarantoolType.NUMBER, JdbcType.REAL, "real"),

    INT(TarantoolType.INTEGER, JdbcType.INTEGER, "int"),
    INTEGER(TarantoolType.INTEGER, JdbcType.INTEGER, "integer"),

    BOOL(TarantoolType.BOOLEAN, JdbcType.BOOLEAN, "bool"),
    BOOLEAN(TarantoolType.BOOLEAN, JdbcType.BOOLEAN, "boolean"),

    VARCHAR(TarantoolType.STRING, JdbcType.VARCHAR, "varchar") {
        @Override
        public String getDisplayType() {
            return getTypeName() + "(128)";
        }
    },
    TEXT(TarantoolType.STRING, JdbcType.VARCHAR, "text"),

    SCALAR(TarantoolType.SCALAR, JdbcType.BINARY, "scalar");

    private static final Map<TarantoolType, TarantoolSqlType> defaultSqlTypeMapping;
    static {
        defaultSqlTypeMapping = new HashMap<>();
        defaultSqlTypeMapping.put(TarantoolType.BOOLEAN, TarantoolSqlType.BOOLEAN);
        defaultSqlTypeMapping.put(TarantoolType.STRING, TarantoolSqlType.VARCHAR);
        defaultSqlTypeMapping.put(TarantoolType.INTEGER, TarantoolSqlType.INTEGER);
        defaultSqlTypeMapping.put(TarantoolType.NUMBER, TarantoolSqlType.DOUBLE);
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

    public boolean isTrimmable() {
        return jdbcType.isTrimmable();
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
