package org.tarantool.jdbc;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class SqlTestUtils {

    public static String makeDefaultJdbcUrl() {
        return makeJdbcUrl(
            System.getProperty("tntHost", "localhost"),
            Integer.valueOf(System.getProperty("tntPort", "3301")),
            System.getProperty("tntUser", "test_admin"),
            System.getProperty("tntPass", "4pWBZmLEgkmKK5WP")
        );
    }

    public static String makeJdbcUrl(String host, int port, String user, String password) {
        return String.format("jdbc:tarantool://%s:%d?user=%s&password=%s", host, port, user, password);
    }

    public static List<List<Object>> makeSingletonListResult(Object... rows) {
        List<List<Object>> result = new ArrayList<>();
        for (Object row : rows) {
            result.add(Collections.singletonList(row));
        }
        return result;
    }

}
