package org.tarantool.jdbc.cursor;

import java.sql.SQLException;
import java.util.List;

/**
 *  Scrollable iterator to support {@link java.sql.ResultSet#TYPE_SCROLL_INSENSITIVE}
 *  result set type semantic.
 */
public class InMemoryScrollableCursorIteratorImpl extends InMemoryForwardCursorIteratorImpl {

    public InMemoryScrollableCursorIteratorImpl(List<List<Object>> results) {
        super(results);
    }

    @Override
    public void beforeFirst() throws SQLException {
        moveIfHasResults(-1);
    }

    @Override
    public void afterLast() throws SQLException {
        moveIfHasResults(results.size());
    }

    @Override
    public boolean first() throws SQLException {
        return moveIfHasResults(0);
    }

    @Override
    public boolean last() throws SQLException {
        return moveIfHasResults(results.size() - 1);
    }

    @Override
    public boolean absolute(int row) throws SQLException {
        if (!hasResults()) {
            return false;
        }
        if (row == 0) {
            beforeFirst();
            return false;
        }
        if (row > results.size()) {
            afterLast();
            return false;
        }
        if (row < -results.size()) {
            beforeFirst();
            return false;
        }

        currentPosition = (row > 0) ? row - 1 : results.size() + row;
        return true;
    }

    @Override
    public boolean relative(int rows) throws SQLException {
        if (!hasResults()) {
            return false;
        }
        if (rows == 0) {
            return !(isBeforeFirst() || isAfterLast());
        }
        if (currentPosition + rows >= results.size()) {
            afterLast();
            return false;
        }
        if (currentPosition + rows <= -1) {
            beforeFirst();
            return false;
        }

        return absolute(currentPosition + rows + 1);
    }

    @Override
    public boolean previous() throws SQLException {
        if (!hasResults() || isBeforeFirst()) {
            return false;
        }
        currentPosition--;
        return !isBeforeFirst();
    }

    /**
     * Moves to the target position if results is not empty.
     *
     * @param position target position
     * @return successful operation status
     */
    private boolean moveIfHasResults(int position) {
        if (!hasResults()) {
            return false;
        }
        currentPosition = position;
        return true;
    }
}
