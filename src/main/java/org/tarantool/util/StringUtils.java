package org.tarantool.util;

public class StringUtils {

    public static boolean isEmpty(String string) {
        return (string == null) || (string.isEmpty());
    }

    public static boolean isNotEmpty(String string) {
        return !isEmpty(string);
    }

    public static boolean isBlank(String string) {
        return (string == null) || (string.trim().isEmpty());
    }

    public static boolean isNotBlank(String string) {
        return !isBlank(string);
    }

}
