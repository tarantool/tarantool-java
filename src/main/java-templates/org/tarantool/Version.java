package org.tarantool;

public final class Version {
    public static final String version = "${project.version}";
    public static final int majorVersion = Integer.parseInt("${parsedVersion.majorVersion}");
    public static final int minorVersion = Integer.parseInt("${parsedVersion.minorVersion}");
}
