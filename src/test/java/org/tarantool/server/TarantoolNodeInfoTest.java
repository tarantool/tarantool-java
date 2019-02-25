package org.tarantool.server;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static org.junit.jupiter.api.Assertions.assertThrows;

class TarantoolNodeInfoTest {

    @DisplayName("Test that a TarantoolNodeInfo throws an illegal argument exception" +
            "in case when it's being created with wrong address string")
    @ParameterizedTest
    @ValueSource(strings = {
            "hostname: 333",
            "127.0.0.1:333333"
    })
    void testThrowsExceptionInCaseOfInvalidStringAddress(String address) {
        assertThrows(IllegalArgumentException.class,
                () -> TarantoolNodeInfo.create(address),
                "We expect the code under test to throw an IllegalArgumentException, but it didn't");
    }
}