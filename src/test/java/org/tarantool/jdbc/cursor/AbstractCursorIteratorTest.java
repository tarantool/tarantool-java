package org.tarantool.jdbc.cursor;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 *  Minimal iterator use cases should be implemented by every {@link CursorIterator}.
 */
public abstract class AbstractCursorIteratorTest {

    protected abstract CursorIterator<List<Object>> getCursorIterator(List<List<Object>> result);

    protected List<List<Object>> makeSingletonListResult(Object... rows) {
        List<List<Object>> result = new ArrayList<>();
        for (Object row : rows) {
            result.add(Collections.singletonList(row));
        }
        return result;
    }

    @Test
    @DisplayName("failed with a null result object")
    void testFailIteratorWithNullResult() {
        assertThrows(IllegalArgumentException.class, () -> new InMemoryForwardCursorIteratorImpl(null));
    }

    @Test
    @DisplayName("iterated through an empty result")
    void testIterationOverEmptyResult() throws SQLException {
        List<List<Object>> result = makeSingletonListResult();
        CursorIterator<List<Object>> iterator = getCursorIterator(result);

        assertEmpty(iterator);

        for (int i = 0; i < 10; i++) {
            assertFalse(iterator.next());
            assertEmpty(iterator);
        }
    }

    @Test
    @DisplayName("iterated through the non-empty results")
    void testUseCases() throws SQLException {
        List<List<Object>> result = makeSingletonListResult("1");
        forwardIteratorUseCase(getCursorIterator(result), result);

        result = makeSingletonListResult("1", "2");
        forwardIteratorUseCase(getCursorIterator(result), result);

        result = makeSingletonListResult("1", "2", "3");
        forwardIteratorUseCase(getCursorIterator(result), result);

        result = makeSingletonListResult("1", "2", "3", "4", "5", "6", "7", "8", "9");
        forwardIteratorUseCase(getCursorIterator(result), result);
    }

    /**
     * Tests an expected behaviour from a forward iterator.
     *
     * @param iterator forward iterator to be tested
     * @param result   result is backed by <code>iterator</code>
     */
    protected void forwardIteratorUseCase(CursorIterator<List<Object>> iterator, List<List<Object>> result)
            throws SQLException {
        assertFalse(result.isEmpty());

        assertBeforeFirst(iterator);

        for (int i = 0; i < result.size(); i++) {
            assertTrue(iterator.next());
            assertNthPosition(i + 1, iterator, result);
        }

        assertFalse(iterator.next()); // after last
        assertAfterLast(iterator);

        for (int i = 0; i < 10; i++) {
            assertFalse(iterator.next());
            assertAfterLast(iterator);
        }
    }

    protected void assertBeforeFirst(CursorIterator<List<Object>> iterator) throws SQLException {
        assertTrue(iterator.isBeforeFirst());
        assertFalse(iterator.isFirst());
        assertFalse(iterator.isLast());
        assertFalse(iterator.isAfterLast());
        assertEquals(0, iterator.getRow());
        assertThrows(SQLException.class, iterator::getItem);
    }

    protected void assertAfterLast(CursorIterator<List<Object>> iterator) throws SQLException {
        assertFalse(iterator.isBeforeFirst());
        assertFalse(iterator.isFirst());
        assertFalse(iterator.isLast());
        assertTrue(iterator.isAfterLast());
        assertEquals(0, iterator.getRow());
        assertThrows(SQLException.class, iterator::getItem);
    }

    protected void assertFirst(CursorIterator<List<Object>> iterator, List<List<Object>> result) throws SQLException {
        assertFalse(iterator.isBeforeFirst());
        assertTrue(iterator.isFirst());
        assertFalse(iterator.isAfterLast());
        assertEquals(1, iterator.getRow());
        assertEquals(result.get(0), iterator.getItem());
    }

    protected void assertLast(CursorIterator<List<Object>> iterator, List<List<Object>> result) throws SQLException {
        assertFalse(iterator.isBeforeFirst());
        assertTrue(iterator.isLast());
        assertFalse(iterator.isAfterLast());
        assertEquals(result.size(), iterator.getRow());
        assertEquals(result.get(result.size() - 1), iterator.getItem());
    }

    protected void assertNthPosition(int position, CursorIterator<List<Object>> iterator, List<List<Object>> result)
        throws SQLException {
        assertFalse(iterator.isBeforeFirst());
        assertFalse(iterator.isAfterLast());
        assertEquals(position, iterator.getRow());
        assertEquals(result.get(position - 1), iterator.getItem());
    }

    protected void assertEmpty(CursorIterator<List<Object>> iterator) throws SQLException {
        assertFalse(iterator.isBeforeFirst());
        assertFalse(iterator.isFirst());
        assertFalse(iterator.isLast());
        assertFalse(iterator.isAfterLast());
        assertEquals(0, iterator.getRow());
        assertThrows(SQLException.class, iterator::getItem);
    }

}
