package org.tarantool.jdbc;

import org.tarantool.util.SQLStates;

import org.junit.jupiter.api.Assertions;

import java.sql.SQLException;

public class SqlAssertions {

    public static void assertSqlExceptionHasStatus(SQLException exception, SQLStates state) {
        Assertions.assertEquals(state.getSqlState(), exception.getSQLState());
    }

}
