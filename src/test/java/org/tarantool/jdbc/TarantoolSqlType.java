package org.tarantool.jdbc;

/**
 * Enumeration of SQL types recognizable by tarantool.
 */
public enum TarantoolSqlType {

    FLOAT("FLOAT"),
    DOUBLE("DOUBLE"),
    REAL("REAL"),

    INT("INT"),
    INTEGER("INTEGER"),

    VARCHAR("VARCHAR(128)"),
    TEXT("TEXT"),

    SCALAR("SCALAR");

    //TIMESTAMP("TIMESTAMP"),

    public String sqlType;

    TarantoolSqlType(String sqlType) {
        this.sqlType = sqlType;
    }
}
