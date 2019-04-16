package org.tarantool;

import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

class IteratorTest {
    protected class MockOps extends AbstractTarantoolOps<Integer, List<?>, Object, List<?>> {

        @Override
        public List exec(Code code, Object... args) {
            return null;
        }

        @Override
        public void close() {
            throw new UnsupportedOperationException();
        }
    }

    @Test
    void testSelectWithIteratorInsteadOfInteger() {
        MockOps ops = new MockOps();
        MockOps spyOps = spy(ops);

        spyOps.select(1, 1, new ArrayList<Integer>(), 0, 1, Iterator.EQ);

        verify(spyOps, times(1)).select(1, 1, new ArrayList<Integer>(), 0, 1, 0);
    }
}
