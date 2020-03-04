package org.tarantool.util;

import java.util.Objects;

/**
 * Server version holder.
 */
public class ServerVersion implements Comparable<ServerVersion> {

    public static final ServerVersion V_1_9 = new ServerVersion(1, 9, 0);
    public static final ServerVersion V_1_10 = new ServerVersion(1, 10, 0);
    public static final ServerVersion V_2_1 = new ServerVersion(2, 1, 0);
    public static final ServerVersion V_2_2 = new ServerVersion(2, 2, 0);
    public static final ServerVersion V_2_2_1 = new ServerVersion(2, 2, 1);
    public static final ServerVersion V_2_3 = new ServerVersion(2, 3, 0);

    private final int majorVersion;
    private final int minorVersion;
    private final int patchVersion;

    /**
     * Makes a parsed server version container from
     * a string in format like {@code MAJOR.MINOR.PATCH[-BUILD-gCOMMIT]}.
     *
     * @param version string in the Tarantool version format.
     */
    public ServerVersion(String version) {
        String[] parts = splitVersionParts(version);
        if (parts.length < 3) {
            throw new IllegalArgumentException("Expected at least major, minor, and patch version parts");
        }
        this.majorVersion = Integer.parseInt(parts[0]);
        this.minorVersion = Integer.parseInt(parts[1]);
        this.patchVersion = Integer.parseInt(parts[2]);
    }

    public ServerVersion(int majorVersion,
                         int minorVersion,
                         int patchVersion) {
        this.majorVersion = majorVersion;
        this.minorVersion = minorVersion;
        this.patchVersion = patchVersion;
    }

    public int getMajorVersion() {
        return majorVersion;
    }

    public int getMinorVersion() {
        return minorVersion;
    }

    public int getPatchVersion() {
        return patchVersion;
    }

    public boolean isEqual(String versionString) {
        return isEqual(new ServerVersion(versionString));
    }

    public boolean isEqual(ServerVersion version) {
        return compareTo(version) == 0;
    }

    public boolean isLessOrEqualThan(String versionString) {
        return isLessOrEqualThan(new ServerVersion(versionString));
    }

    public boolean isLessOrEqualThan(ServerVersion version) {
        return compareTo(version) <= 0;
    }

    public boolean isGreaterOrEqualThan(String versionString) {
        return isGreaterOrEqualThan(new ServerVersion(versionString));
    }

    public boolean isGreaterOrEqualThan(ServerVersion version) {
        return compareTo(version) >= 0;
    }

    public boolean isGreaterThan(String versionString) {
        return isGreaterThan(new ServerVersion(versionString));
    }

    public boolean isGreaterThan(ServerVersion version) {
        return compareTo(version) > 0;
    }

    public boolean isLessThan(String versionString) {
        return isLessThan(new ServerVersion(versionString));
    }

    public boolean isLessThan(ServerVersion version) {
        return compareTo(version) < 0;
    }

    @Override
    public int compareTo(ServerVersion that) {
        return Integer.compare(this.toNumber(), that.toNumber());
    }

    @Override
    public boolean equals(Object object) {
        if (this == object) {
            return true;
        }
        if (object == null || getClass() != object.getClass()) {
            return false;
        }
        ServerVersion that = (ServerVersion) object;
        return majorVersion == that.majorVersion &&
            minorVersion == that.minorVersion &&
            patchVersion == that.patchVersion;
    }

    @Override
    public int hashCode() {
        return Objects.hash(majorVersion, minorVersion, patchVersion);
    }

    /**
     * Translates version parts to format XXXYYYZZZ.
     * For example, {@code 1.2.3} translates to number {@code 1002003}
     *
     * @return version as number
     */
    private int toNumber() {
        return (majorVersion * 1000 + minorVersion) * 1000 + patchVersion;
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
