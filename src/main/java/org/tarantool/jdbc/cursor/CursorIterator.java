package org.tarantool.jdbc.cursor;

import java.sql.SQLException;

/**
 * Extracted interface for a cursor traversal part of {@link java.sql.ResultSet}.
 */
public interface CursorIterator<T> {

    boolean isBeforeFirst() throws SQLException;

    boolean isAfterLast() throws SQLException;

    boolean isFirst() throws SQLException;

    boolean isLast() throws SQLException;

    void beforeFirst() throws SQLException;

    void afterLast() throws SQLException;

    boolean first() throws SQLException;

    boolean last() throws SQLException;

    boolean absolute(int row) throws SQLException;

    boolean relative(int rows) throws SQLException;

    boolean next() throws SQLException;

    boolean previous() throws SQLException;

    int getRow() throws SQLException;

    T getItem() throws SQLException;

    void close();

}
