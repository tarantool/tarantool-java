package org.tarantool.jdbc.cursor;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.tarantool.jdbc.SqlAssertions.assertAfterLast;
import static org.tarantool.jdbc.SqlAssertions.assertBeforeFirst;
import static org.tarantool.jdbc.SqlAssertions.assertEmpty;
import static org.tarantool.jdbc.SqlAssertions.assertNthPosition;
import static org.tarantool.jdbc.SqlTestUtils.makeSingletonListResult;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.sql.SQLException;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Stream;

/**
 * Minimal iterator use cases should be implemented by every {@link CursorIterator}.
 *
 * NOTE: Parametrized tests can be simplified after
 * https://github.com/junit-team/junit5/issues/878
 */
@DisplayName("A generic cursor")
class GenericCursorIteratorTest {

    static Stream<CursorFactory> getCursorFactories() {
        return Stream.of(
            InMemoryForwardCursorIteratorImpl::new,
            InMemoryScrollableCursorIteratorImpl::new
        );
    }

    @ParameterizedTest
    @MethodSource("getCursorFactories")
    @DisplayName("failed with a null result object")
    void testFailIteratorWithNullResult(CursorFactory factory) {
        assertThrows(IllegalArgumentException.class, () ->  factory.apply(null));
    }

    @ParameterizedTest
    @MethodSource("getCursorFactories")
    @DisplayName("iterated through an empty result")
    void testIterationOverEmptyResult(CursorFactory factory) throws SQLException {
        List<List<Object>> result = makeSingletonListResult();
        CursorIterator<List<Object>> iterator = factory.apply(result);

        assertEmpty(iterator);

        for (int i = 0; i < 10; i++) {
            assertFalse(iterator.next());
            assertEmpty(iterator);
        }
    }

    @ParameterizedTest
    @MethodSource("getCursorFactories")
    @DisplayName("iterated through the non-empty results")
    void testUseCases(CursorFactory factory) throws SQLException {
        List<List<Object>> result = makeSingletonListResult("1");
        forwardIteratorUseCase(factory.apply(result), result);

        result = makeSingletonListResult("1", "2");
        forwardIteratorUseCase(factory.apply(result), result);

        result = makeSingletonListResult("1", "2", "3");
        forwardIteratorUseCase(factory.apply(result), result);

        result = makeSingletonListResult("1", "2", "3", "4", "5", "6", "7", "8", "9");
        forwardIteratorUseCase(factory.apply(result), result);
    }

    /**
     * Tests an expected behaviour from a forward iterator.
     *
     * @param iterator forward iterator to be tested
     * @param result   result is backed by <code>iterator</code>
     */
    private void forwardIteratorUseCase(CursorIterator<List<Object>> iterator, List<List<Object>> result)
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

    private interface CursorFactory extends Function<List<List<Object>>, CursorIterator<List<Object>>> {

    }

}
