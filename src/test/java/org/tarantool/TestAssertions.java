package org.tarantool;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.util.List;
import java.util.function.Supplier;

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

    public static <T> void assertSubArrayEquals(byte[] first, int firstOffset,
                                                byte[] second, int secondOffset,
                                                int length,
                                                Supplier<String> messageSupplier) {
        for (int i = 0; i < length; i++) {
            if (first[firstOffset + i] != second[secondOffset + i]) {
                fail(messageSupplier);
            }
        }
    }
}
