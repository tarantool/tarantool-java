package org.tarantool.util;

public enum SQLStates {

    INVALID_PARAMETER_VALUE("22023"),
    CONNECTION_DOES_NOT_EXIST("08003");

    private final String sqlState;

    SQLStates(String sqlState) {
        this.sqlState = sqlState;
    }

    public String getSqlState() {
        return sqlState;
    }
}
