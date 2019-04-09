package org.tarantool.util;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLNonTransientException;
import java.sql.Statement;

public class JdbcConstants {

    public static void checkGeneratedKeysConstant(int autoGeneratedKeys) throws SQLException {
        if (autoGeneratedKeys != Statement.NO_GENERATED_KEYS &&
            autoGeneratedKeys != Statement.RETURN_GENERATED_KEYS) {
            throw new SQLNonTransientException("", SQLStates.INVALID_PARAMETER_VALUE.getSqlState());
        }
    }

    public static void checkHoldabilityConstant(int holdability) throws SQLException {
        if (holdability != ResultSet.CLOSE_CURSORS_AT_COMMIT &&
            holdability != ResultSet.HOLD_CURSORS_OVER_COMMIT) {
            throw new SQLNonTransientException("", SQLStates.INVALID_PARAMETER_VALUE.getSqlState());
        }
    }

}
