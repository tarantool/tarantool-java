package org.tarantool;

import java.util.function.BiFunction;

public enum ServerVersion {

    V_1_9("1", "9", "0"),
    V_1_10("1", "10", "0"),
    V_2_1("2", "1", "0"),
    V_2_2("2", "2", "0"),
    V_2_2_1("2", "2", "1");

    private final String majorVersion;
    private final String minorVersion;
    private final String patchVersion;

    ServerVersion(String majorVersion,
                  String minorVersion, String patchVersion) {
        this.majorVersion = majorVersion;
        this.minorVersion = minorVersion;
        this.patchVersion = patchVersion;
    }

    public String getMajorVersion() {
        return majorVersion;
    }

    public String getMinorVersion() {
        return minorVersion;
    }

    public String getPatchVersion() {
        return patchVersion;
    }

    public boolean isLessOrEqualThan(String versionString) {
        return compareVersions(versionString, (server, minimal) -> server >= minimal);
    }

    public boolean isGreaterOrEqualThan(String versionString) {
        return compareVersions(versionString, (server, maximal) -> server <= maximal);
    }

    public boolean isGreaterThan(String versionString) {
        return compareVersions(versionString, (server, maximal) -> server < maximal);
    }

    private boolean compareVersions(String versionString, BiFunction<Integer, Integer, Boolean> comparator) {
        int parsedVersion = toNumber(splitVersionParts(versionString));
        int thisVersion = toNumber(new String[] { majorVersion, minorVersion, patchVersion });
        return comparator.apply(parsedVersion, thisVersion);
    }

    /**
     * Translates version parts to format XXXYYYZZZ.
     * For example, {@code 1.2.1} translates to number {@code 1002001}
     *
     * @param parts version parts
     * @return version as number
     */
    private int toNumber(String[] parts) {
        int version = 0;
        for (int i = 0; i < 3; i++) {
            version = (version + Integer.parseInt(parts[i])) * 1000;
        }
        return version / 1000;
    }

    /**
     * Splits Tarantool version string into parts.
     * For example, {@code 2.1.1-423-g4007436aa} => {@code [2, 1, 1, 423, g4007436aa]}.
     *
     * @param version Tarantool version string
     * @return split parts
     */
    private String[] splitVersionParts(String version) {
        return version.split("[.\\-]");
    }
}
