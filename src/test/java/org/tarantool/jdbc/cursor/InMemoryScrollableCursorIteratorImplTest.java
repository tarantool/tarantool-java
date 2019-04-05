package org.tarantool.jdbc.cursor;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.tarantool.TestUtils;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.sql.SQLException;
import java.util.List;

@DisplayName("A scrollable iterator")
class InMemoryScrollableCursorIteratorImplTest extends AbstractCursorIteratorTest {

    @Override
    protected CursorIterator<List<Object>> getCursorIterator(List<List<Object>> result) {
        return new InMemoryScrollableCursorIteratorImpl(result);
    }

    @Test
    @DisplayName("failed with a null result object")
    void testFailIteratorWithNullResult() {
        assertThrows(IllegalArgumentException.class, () -> new InMemoryScrollableCursorIteratorImpl(null));
    }

    @Test
    @DisplayName("moved to the last position")
    void testMoveLast() throws SQLException {
        List<List<Object>> result = makeSingletonListResult("a", "b", "c", "d", "e");
        CursorIterator<List<Object>> iterator = getCursorIterator(result);

        assertBeforeFirst(iterator);
        assertTrue(iterator.last());
        assertLast(iterator, result);
    }

    @Test
    @DisplayName("moved to the first position")
    void testMoveFirst() throws SQLException {
        List<List<Object>> result = makeSingletonListResult("a", "b", "c", "d", "e");
        CursorIterator<List<Object>> iterator = getCursorIterator(result);

        assertBeforeFirst(iterator);
        iterator.afterLast();
        assertAfterLast(iterator);
        assertTrue(iterator.first());
        assertFirst(iterator, result);
    }

    @Test
    @DisplayName("moved to before the first position")
    void testMoveBeforeFirst() throws SQLException {
        List<List<Object>> result = makeSingletonListResult("a", "b", "c", "d", "e");
        CursorIterator<List<Object>> iterator = getCursorIterator(result);

        assertBeforeFirst(iterator);
        iterator.afterLast();
        assertAfterLast(iterator);
        iterator.beforeFirst();
        assertBeforeFirst(iterator);
    }

    @Test
    @DisplayName("moved to after the last position")
    void testMoveAfterLast() throws SQLException {
        List<List<Object>> result = makeSingletonListResult("a", "b", "c", "d", "e");
        CursorIterator<List<Object>> iterator = getCursorIterator(result);

        assertBeforeFirst(iterator);
        iterator.afterLast();
        assertAfterLast(iterator);
    }

    @Test
    @DisplayName("moved to an absolute position")
    void testMoveAbsolute() throws SQLException {
        List<List<Object>> result = makeSingletonListResult("a", "b", "c", "d", "e");
        CursorIterator<List<Object>> iterator = getCursorIterator(result);

        for (int i = 0; i < result.size(); i++) {
            assertTrue(iterator.absolute(i + 1));
            assertNthPosition(i + 1, iterator, result);
        }
        for (int i = result.size() + 1; i < result.size() + 10; i++) {
            assertFalse(iterator.absolute(i));
            assertAfterLast(iterator);
        }
    }

    @Test
    @DisplayName("moved to a negative absolute position (reverse traversal)")
    void testMoveNegativeAbsolute() throws SQLException {
        List<List<Object>> result = makeSingletonListResult("a", "b", "c", "d", "e");
        CursorIterator<List<Object>> iterator = getCursorIterator(result);

        for (int i = 0; i < result.size(); i++) {
            assertTrue(iterator.absolute(-i - 1)); // -1 -2 -3 -4 -5
            assertNthPosition(5 - i, iterator, result); // 5 4 3 2 1
        }
        for (int i = -result.size() - 1; i > -result.size() - 10; i--) {
            assertFalse(iterator.absolute(i));
            assertBeforeFirst(iterator);
        }
    }

    @Test
    @DisplayName("moved to a relative position")
    void testMoveRelative() throws SQLException {
        List<List<Object>> result = makeSingletonListResult("a", "b", "c", "d", "e");
        CursorIterator<List<Object>> iterator = getCursorIterator(result);

        assertBeforeFirst(iterator);
        assertTrue(iterator.relative(3)); // before the first -> 3
        assertNthPosition(3, iterator, result);

        assertTrue(iterator.relative(-2)); // 3 -> 1 (first)
        assertNthPosition(1, iterator, result);

        assertTrue(iterator.relative(4)); // 1 -> 5 (last)
        assertNthPosition(5, iterator, result);

        assertTrue(iterator.relative(-3)); // 5 -> 2
        assertNthPosition(2, iterator, result);

        assertFalse(iterator.relative(-2)); // 2 -> before the first
        assertBeforeFirst(iterator);

        assertFalse(iterator.relative(0)); // before the first -> before the first
        assertBeforeFirst(iterator);

        assertFalse(iterator.relative(-2)); // before the first -> before the first
        assertBeforeFirst(iterator);

        assertTrue(iterator.relative(4)); // before the first -> 4
        assertNthPosition(4, iterator, result);

        assertFalse(iterator.relative(2)); // 4 -> after the last
        assertAfterLast(iterator);

        assertTrue(iterator.relative(-3)); // after the last -> 3
        assertNthPosition(3, iterator, result);

        assertTrue(iterator.relative(0)); // 3 -> 3
        assertNthPosition(3, iterator, result);

        assertTrue(iterator.relative(1)); // 3 -> 4
        assertNthPosition(4, iterator, result);

        assertFalse(iterator.relative(5)); // 4 -> after last
        assertAfterLast(iterator);

        assertTrue(iterator.relative(-4)); // after last -> 2
        assertNthPosition(2, iterator, result);

        assertFalse(iterator.relative(-3)); // 2 -> before first
        assertBeforeFirst(iterator);
    }

    @Test
    @DisplayName("moved to before the first using absolute navigation")
    void testMoveAbsoluteZero() throws SQLException {
        List<List<Object>> result = makeSingletonListResult("a", "b", "c", "d", "e");
        CursorIterator<List<Object>> iterator = getCursorIterator(result);

        assertBeforeFirst(iterator);
        assertTrue(iterator.absolute(3));
        assertNthPosition(3, iterator, result);

        assertFalse(iterator.absolute(0)); // move to the before the first
        assertBeforeFirst(iterator);
    }

    @Test
    @DisplayName("moved to the same positions using an absolute positioning")
    void testMoveAbsoluteSimilarities() throws SQLException {
        List<List<Object>> result = makeSingletonListResult("a", "b", "c", "d", "e");
        CursorIterator<List<Object>> firstIterator = getCursorIterator(result);

        CursorIterator<List<Object>> secondIterator = getCursorIterator(result);

        assertBeforeFirst(firstIterator);
        assertBeforeFirst(secondIterator);

        // absolute(1) is the same as calling first()
        firstIterator.absolute(1);
        secondIterator.first();
        assertFirst(firstIterator, result);
        assertFirst(secondIterator, result);

        // absolute(-1) is the same as calling last()
        firstIterator.absolute(-1);
        secondIterator.last();
        assertLast(firstIterator, result);
        assertLast(secondIterator, result);

        // absolute(0) is the same as calling beforeFirst()
        firstIterator.absolute(0);
        secondIterator.beforeFirst();
        assertBeforeFirst(firstIterator);
        assertBeforeFirst(secondIterator);
    }

    @Test
    @DisplayName("moved to the same positions using an relative positioning")
    void testMoveRelativeSimilarities() throws SQLException {
        List<List<Object>> result = makeSingletonListResult("a", "b", "c", "d", "e");
        CursorIterator<List<Object>> firstIterator = getCursorIterator(result);

        CursorIterator<List<Object>> secondIterator = getCursorIterator(result);

        assertBeforeFirst(firstIterator);
        assertBeforeFirst(secondIterator);

        // relative(1) is the same as calling next()
        for (int i = 0; i < result.size(); i++) {
            assertTrue(firstIterator.relative(1));
            assertTrue(secondIterator.next());
            assertNthPosition(i + 1, firstIterator, result);
            assertNthPosition(i + 1, secondIterator, result);
        }

        assertLast(firstIterator, result);
        assertLast(secondIterator, result);

        // relative(-1) is the same as calling previous()
        for (int i = result.size(); i > 1; i--) {
            assertTrue(firstIterator.relative(-1));
            assertTrue(secondIterator.previous());
            assertNthPosition(i - 1, firstIterator, result);
            assertNthPosition(i - 1, secondIterator, result);
        }

        assertFirst(firstIterator, result);
        assertFirst(secondIterator, result);
    }

    @Test
    @DisplayName("moved to edges over an empty result")
    void testIterationOverEmptyResult() throws SQLException {
        List<List<Object>> result = makeSingletonListResult();
        CursorIterator<List<Object>> iterator = getCursorIterator(result);

        Runnable[] actions = new Runnable[] {
                TestUtils.throwingWrapper(iterator::beforeFirst),
                TestUtils.throwingWrapper(iterator::afterLast),
                TestUtils.throwingWrapper(iterator::first),
                TestUtils.throwingWrapper(iterator::last),
                TestUtils.throwingWrapper(iterator::previous),
                TestUtils.throwingWrapper(() -> iterator.relative(1)),
                TestUtils.throwingWrapper(() -> iterator.absolute(1)),
        };

        for (Runnable action : actions) {
            assertEmpty(iterator);
            action.run();
        }
    }

    @Test
    @DisplayName("iterated through the non-empty results")
    void testUseCases() throws SQLException {
        List<List<Object>> result = makeSingletonListResult("a");
        backwardIteratorUseCase(getCursorIterator(result), result);

        result = makeSingletonListResult("a", "b");
        backwardIteratorUseCase(getCursorIterator(result), result);

        result = makeSingletonListResult("a", "b", "c");
        backwardIteratorUseCase(getCursorIterator(result), result);

        result = makeSingletonListResult("a", "b", "c", "d", "e", "f", "g", "h", "i");
        backwardIteratorUseCase(getCursorIterator(result), result);
    }

    /**
     * Tests an expected behaviour from a scrollable iterator.
     *
     * @param iterator scrollable iterator to be tested
     * @param result   result is backed by <code>iterator</code>
     */
    protected void backwardIteratorUseCase(CursorIterator<List<Object>> iterator, List<List<Object>> result)
            throws SQLException {
        assertFalse(result.isEmpty());

        assertBeforeFirst(iterator);
        iterator.afterLast();
        assertAfterLast(iterator);

        for (int i = result.size() - 1; i >= 0; i--) {
            assertTrue(iterator.previous());
            assertNthPosition(i + 1, iterator, result);
        }

        assertFalse(iterator.previous()); // before first
        assertBeforeFirst(iterator);

        for (int i = 0; i < 10; i++) {
            assertFalse(iterator.previous());
            assertBeforeFirst(iterator);
        }
    }

}
