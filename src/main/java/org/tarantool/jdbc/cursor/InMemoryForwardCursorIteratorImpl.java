package org.tarantool.jdbc.cursor;

import org.tarantool.util.SQLStates;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

/**
 *  Forward only iterator to support {@link java.sql.ResultSet#TYPE_FORWARD_ONLY}
 *  result set semantic.
 */
public class InMemoryForwardCursorIteratorImpl implements CursorIterator<List<Object>> {

    protected final List<List<Object>> results = new ArrayList<>();
    protected int currentPosition = -1;

    public InMemoryForwardCursorIteratorImpl(List<List<Object>> results) {
        if (results == null) {
            throw new IllegalArgumentException("Results list cannot be null");
        }
        this.results.addAll(results);
    }

    @Override
    public boolean isBeforeFirst() throws SQLException {
        return hasResults() && currentPosition == -1;
    }

    @Override
    public boolean isAfterLast() throws SQLException {
        return hasResults() && currentPosition == results.size();
    }

    @Override
    public boolean isFirst() throws SQLException {
        return hasResults() && currentPosition == 0;
    }

    @Override
    public boolean isLast() throws SQLException {
        return hasResults() && currentPosition == results.size() - 1;
    }

    @Override
    public void beforeFirst() throws SQLException {
        throw new SQLException(
                "Cannot be called on forward only cursor",
                SQLStates.INVALID_CURSOR_STATE.getSqlState()
        );
    }

    @Override
    public void afterLast() throws SQLException {
        throw new SQLException(
                "Cannot be called on forward only cursor",
                SQLStates.INVALID_CURSOR_STATE.getSqlState()
        );
    }

    @Override
    public boolean first() throws SQLException {
        throw new SQLException(
                "Cannot be called on forward only cursor",
                SQLStates.INVALID_CURSOR_STATE.getSqlState()
        );
    }

    @Override
    public boolean last() throws SQLException {
        throw new SQLException(
                "Cannot be called on forward only cursor",
                SQLStates.INVALID_CURSOR_STATE.getSqlState()
        );
    }

    @Override
    public boolean absolute(int row) throws SQLException {
        throw new SQLException(
                "Cannot be called on forward only cursor",
                SQLStates.INVALID_CURSOR_STATE.getSqlState()
        );
    }

    @Override
    public boolean relative(int rows) throws SQLException {
        throw new SQLException(
                "Cannot be called on forward only cursor",
                SQLStates.INVALID_CURSOR_STATE.getSqlState()
        );
    }

    @Override
    public boolean next() throws SQLException {
        if (!hasResults() || isAfterLast()) {
            return false;
        }
        currentPosition++;
        return !isAfterLast();
    }

    @Override
    public boolean previous() throws SQLException {
        throw new SQLException(
                "Cannot be called on forward only cursor",
                SQLStates.INVALID_CURSOR_STATE.getSqlState()
        );
    }

    @Override
    public int getRow() throws SQLException {
        return !hasResults() || isBeforeFirst() || isAfterLast() ? 0 : currentPosition + 1;
    }

    @Override
    public List<Object> getItem() throws SQLException {
        int row = getRow();
        if (row > 0) {
            return results.get(row - 1);
        }
        throw new SQLException(
                "Cursor is out of range. Try to call next() or previous() before.",
                SQLStates.INVALID_CURSOR_STATE.getSqlState()
        );
    }

    protected boolean hasResults() {
        return !results.isEmpty();
    }

    @Override
    public void close() {
        results.clear();
        currentPosition = -1;
    }

}
