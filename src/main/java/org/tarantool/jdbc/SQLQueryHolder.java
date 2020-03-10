package org.tarantool.jdbc;

import java.util.Arrays;
import java.util.List;

public class SQLQueryHolder {

    private final long statementId;
    private final String query;
    private final List<Object> params;

    public static SQLQueryHolder of(String query, Object... params) {
        return new SQLQueryHolder(0L, query, Arrays.asList(params));
    }

    public static SQLQueryHolder of(long statementId, String query, Object... params) {
        return new SQLQueryHolder(statementId, query, Arrays.asList(params));
    }

    private SQLQueryHolder(Long statementId, String query, List<Object> params) {
        this.statementId = statementId;
        this.query = query;
        this.params = params;
    }

    public String getQuery() {
        return query;
    }

    public Long getStatementId() {
        return statementId;
    }

    public List<Object> getParams() {
        return params;
    }

    public boolean isPrepared() {
        return statementId != 0;
    }

}
