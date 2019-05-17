package org.tarantool;

import org.junit.jupiter.api.Assumptions;

public class TestAssumptions {

    public static void assumeMinimalServerVersion(TarantoolConsole console, ServerVersion version) {
        Assumptions.assumeTrue(version.haveMinimalVersion(TestUtils.getTarantoolVersion(console)));
    }

    public static void assumeMaximalServerVersion(TarantoolConsole console, ServerVersion version) {
        Assumptions.assumeTrue(version.haveMaximalVersion(TestUtils.getTarantoolVersion(console)));
    }

}
