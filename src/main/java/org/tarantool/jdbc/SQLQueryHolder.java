package org.tarantool.jdbc;

import java.util.Arrays;
import java.util.List;

public class SQLQueryHolder {

    private final String query;
    private final List<Object> params;

    public static SQLQueryHolder of(String query, Object... params) {
        return new SQLQueryHolder(query, Arrays.asList(params));
    }

    private SQLQueryHolder(String query, List<Object> params) {
        this.query = query;
        this.params = params;
    }

    public String getQuery() {
        return query;
    }

    public List<Object> getParams() {
        return params;
    }

}
