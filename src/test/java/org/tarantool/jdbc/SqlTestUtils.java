package org.tarantool.jdbc;

import org.tarantool.TarantoolTestHelper;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SqlTestUtils {

    public static String makeDefaultJdbcUrl() {
        Map<String, String> parameters = new HashMap<>();
        parameters.put(SQLProperty.USER.getName(), TarantoolTestHelper.USERNAME);
        parameters.put(SQLProperty.PASSWORD.getName(), TarantoolTestHelper.PASSWORD);
        return makeJdbcUrl(
            TarantoolTestHelper.HOST + ":" + TarantoolTestHelper.PORT,
            parameters
        );
    }

    public static String makeDefaulJdbcUrl(String address, Map<String, String> parameters) {
        Map<String, String> params = new HashMap<>(parameters);
        params.put(SQLProperty.USER.getName(), TarantoolTestHelper.USERNAME);
        params.put(SQLProperty.PASSWORD.getName(), TarantoolTestHelper.PASSWORD);
        return makeJdbcUrl(address, params);
    }

    public static String makeJdbcUrl(String address, Map<String, String> parameters) {
        StringBuilder url = new StringBuilder("jdbc:tarantool://");
        url.append(address).append("?");
        parameters.forEach((k, v) -> {
            url.append(k)
                .append("=")
                .append(v)
                .append("&");
        });
        url.setLength(url.length() - 1);
        return url.toString();
    }

    public static List<List<Object>> makeSingletonListResult(Object... rows) {
        List<List<Object>> result = new ArrayList<>();
        for (Object row : rows) {
            result.add(Collections.singletonList(row));
        }
        return result;
    }

}
