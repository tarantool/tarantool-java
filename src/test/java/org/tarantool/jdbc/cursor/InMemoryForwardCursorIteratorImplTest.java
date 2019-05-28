package org.tarantool.jdbc.cursor;

import static org.junit.jupiter.api.Assertions.assertThrows;

import org.tarantool.jdbc.SqlTestUtils;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.sql.SQLException;
import java.util.List;

@DisplayName("A forward iterator")
class InMemoryForwardCursorIteratorImplTest {

    @Test
    @DisplayName("failed trying to use unsupported operations")
    void testUnsupportedOperations() {
        List<List<Object>> result = SqlTestUtils.makeSingletonListResult("1");
        CursorIterator<List<Object>> iterator = new InMemoryForwardCursorIteratorImpl(result);

        assertThrows(SQLException.class, iterator::beforeFirst);
        assertThrows(SQLException.class, iterator::afterLast);
        assertThrows(SQLException.class, iterator::first);
        assertThrows(SQLException.class, iterator::last);
        assertThrows(SQLException.class, () -> iterator.absolute(0));
        assertThrows(SQLException.class, () -> iterator.absolute(Integer.MIN_VALUE));
        assertThrows(SQLException.class, () -> iterator.absolute(Integer.MAX_VALUE));
        assertThrows(SQLException.class, () -> iterator.relative(0));
        assertThrows(SQLException.class, () -> iterator.relative(Integer.MIN_VALUE));
        assertThrows(SQLException.class, () -> iterator.relative(Integer.MAX_VALUE));
        assertThrows(SQLException.class, iterator::previous);
    }

}
