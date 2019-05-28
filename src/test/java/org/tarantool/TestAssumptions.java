package org.tarantool;

import org.junit.jupiter.api.Assumptions;

public class TestAssumptions {

    public static void assumeMinimalServerVersion(String rawVersion, ServerVersion version) {
        Assumptions.assumeTrue(version.haveMinimalVersion(rawVersion));
    }

    public static void assumeMaximalServerVersion(String rawVersion, ServerVersion version) {
        Assumptions.assumeTrue(version.haveMaximalVersion(rawVersion));
    }

}
