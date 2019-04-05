package org.tarantool.util;

public enum SQLStates {

    TOO_MANY_RESULTS("0100E"),
    NO_DATA("02000"),
    CONNECTION_DOES_NOT_EXIST("08003"),
    INVALID_PARAMETER_VALUE("22023"),
    INVALID_CURSOR_STATE("24000");

    private final String sqlState;

    SQLStates(String sqlState) {
        this.sqlState = sqlState;
    }

    public String getSqlState() {
        return sqlState;
    }
}
