package org.tarantool;

import org.tarantool.util.ServerVersion;

import org.junit.jupiter.api.Assumptions;

public class TestAssumptions {

    public static void assumeMinimalServerVersion(String rawVersion, ServerVersion version) {
        Assumptions.assumeTrue(version.isLessOrEqualThan(rawVersion));
    }

    public static void assumeMaximalServerVersion(String rawVersion, ServerVersion version) {
        Assumptions.assumeTrue(version.isGreaterOrEqualThan(rawVersion));
    }

    public static void assumeServerVersionLessThan(String rawVersion, ServerVersion version) {
        Assumptions.assumeTrue(version.isGreaterThan(rawVersion));
    }

    public static void assumeServerVersionOutOfRange(String rawVersion,
                                                     ServerVersion left,
                                                     ServerVersion right) {
        Assumptions.assumeFalse(left.isLessOrEqualThan(rawVersion) && right.isGreaterThan(rawVersion));
    }

}
