package org.tarantool.util;

import java.util.stream.Collectors;
import java.util.stream.Stream;

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

    public static String toCsvList(Enum<?>[] values) {
        return Stream.of(values)
            .map(Enum::name)
            .collect(Collectors.joining(","));
    }

}
