package org.tarantool.jdbc;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;

/**
 * Tarantool specific result set metadata extensions.
 */
public interface TarantoolResultSetMetaData extends ResultSetMetaData {

    /**
     * Checks a column accessibility.
     *
     * @param columnIndex column number
     * @throws SQLException if column is not accessible by the index
     */
    void checkColumnIndex(int columnIndex) throws SQLException;

    /**
     * Determines whether a column type can be trimmed or not.
     * This status depends on JDBC types that are defined as
     * trimmable such as {@literal VARCHAR} or {@literal BINARY}
     *
     * @param columnIndex column number
     * @return {@literal true} if the column is trimmable
     * @throws SQLException if column is not accessible by the index
     *
     * @see java.sql.Statement#setMaxFieldSize(int)
     */
    boolean isTrimmable(int columnIndex) throws SQLException;

}
