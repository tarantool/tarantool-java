package org.tarantool.jdbc;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.tarantool.jdbc.cursor.CursorIterator;
import org.tarantool.util.SQLStates;

import org.junit.jupiter.api.Assertions;

import java.sql.SQLException;
import java.util.List;

public class SqlAssertions {

    public static void assertSqlExceptionHasStatus(SQLException exception, SQLStates state) {
        Assertions.assertEquals(state.getSqlState(), exception.getSQLState());
    }

    public static void assertBeforeFirst(CursorIterator<List<Object>> iterator) throws SQLException {
        assertTrue(iterator.isBeforeFirst());
        assertFalse(iterator.isFirst());
        assertFalse(iterator.isLast());
        assertFalse(iterator.isAfterLast());
        assertEquals(0, iterator.getRow());
        assertThrows(SQLException.class, iterator::getItem);
    }

    public static void assertAfterLast(CursorIterator<List<Object>> iterator) throws SQLException {
        assertFalse(iterator.isBeforeFirst());
        assertFalse(iterator.isFirst());
        assertFalse(iterator.isLast());
        assertTrue(iterator.isAfterLast());
        assertEquals(0, iterator.getRow());
        assertThrows(SQLException.class, iterator::getItem);
    }

    public static void assertFirst(CursorIterator<List<Object>> iterator, List<List<Object>> result)
        throws SQLException {
        assertFalse(iterator.isBeforeFirst());
        assertTrue(iterator.isFirst());
        assertFalse(iterator.isAfterLast());
        assertEquals(1, iterator.getRow());
        assertEquals(result.get(0), iterator.getItem());
    }

    public static void assertLast(CursorIterator<List<Object>> iterator, List<List<Object>> result)
        throws SQLException {
        assertFalse(iterator.isBeforeFirst());
        assertTrue(iterator.isLast());
        assertFalse(iterator.isAfterLast());
        assertEquals(result.size(), iterator.getRow());
        assertEquals(result.get(result.size() - 1), iterator.getItem());
    }

    public static void assertNthPosition(int position, CursorIterator<List<Object>> iterator, List<List<Object>> result)
        throws SQLException {
        assertFalse(iterator.isBeforeFirst());
        assertFalse(iterator.isAfterLast());
        assertEquals(position, iterator.getRow());
        assertEquals(result.get(position - 1), iterator.getItem());
    }

    public static void assertEmpty(CursorIterator<List<Object>> iterator) throws SQLException {
        assertFalse(iterator.isBeforeFirst());
        assertFalse(iterator.isFirst());
        assertFalse(iterator.isLast());
        assertFalse(iterator.isAfterLast());
        assertEquals(0, iterator.getRow());
        assertThrows(SQLException.class, iterator::getItem);
    }

}
