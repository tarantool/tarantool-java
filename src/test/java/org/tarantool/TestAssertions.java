package org.tarantool;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;

public class TestAssertions {

    public static void checkRawTupleResult(Object result, List<?> tuple) {
        assertNotNull(result);
        assertTrue(List.class.isAssignableFrom(result.getClass()));
        List<?> list = (List<?>) result;
        assertEquals(1, list.size());
        assertNotNull(list.get(0));
        assertTrue(List.class.isAssignableFrom(list.get(0).getClass()));
        assertEquals(tuple, list.get(0));
    }

}
