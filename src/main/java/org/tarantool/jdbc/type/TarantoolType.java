package org.tarantool.jdbc.type;

/**
 * Tarantool raw NoSQL type.
 */
public enum TarantoolType {

    UNKNOWN("unknown", false, false, 0, 0, 0),
    BOOLEAN("boolean", false, false, 1, 0, 5),
    STRING("string", false, true, Integer.MAX_VALUE, 0, Integer.MAX_VALUE),
    // precision is 20 due to Tarantool integer type has range [-2^63-1..2^64-1]
    INTEGER("integer", true, false, 20, 0, 20),
    // precision is 20 due to Tarantool allows both integer and floating-point values under number type
    NUMBER("number", true, false, 20, 16, 24),
    SCALAR("scalar", false, true, Integer.MAX_VALUE, 0, Integer.MAX_VALUE);

    private final String typeName;
    private final boolean signed;
    private final boolean caseSensitive;
    private final int precision;
    private final int scale;
    private final int displaySize;

    public static TarantoolType of(String type) {
        for (TarantoolType value : TarantoolType.values()) {
            if (value.typeName.equalsIgnoreCase(type)) {
                return value;
            }
        }
        return UNKNOWN;
    }

    TarantoolType(String typeName,
                  boolean signed,
                  boolean caseSensitive,
                  int precision,
                  int scale,
                  int displaySize) {
        this.typeName = typeName;
        this.signed = signed;
        this.caseSensitive = caseSensitive;
        this.precision = precision;
        this.scale = scale;
        this.displaySize = displaySize;
    }

    public String getTypeName() {
        return typeName;
    }

    public boolean isSigned() {
        return signed;
    }

    public boolean isCaseSensitive() {
        return caseSensitive;
    }

    public int getPrecision() {
        return precision;
    }

    public int getScale() {
        return scale;
    }

    public int getDisplaySize() {
        return displaySize;
    }

    @Override
    public String toString() {
        return "TarantoolType{" +
            "typeName='" + typeName + '\'' +
            ", signed=" + signed +
            ", caseSensitive=" + caseSensitive +
            ", precision=" + precision +
            ", scale=" + scale +
            ", displaySize=" + displaySize +
            '}';
    }
}
