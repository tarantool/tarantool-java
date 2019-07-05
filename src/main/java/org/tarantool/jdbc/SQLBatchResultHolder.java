package org.tarantool.jdbc;

import java.util.List;

/**
 * Wrapper for batch SQL query results.
 */
public class SQLBatchResultHolder {

    private final List<SQLResultHolder> results;
    private final Exception error;

    public SQLBatchResultHolder(List<SQLResultHolder> results, Exception error) {
        this.results = results;
        this.error = error;
    }

    public List<SQLResultHolder> getResults() {
        return results;
    }

    public Exception getError() {
        return error;
    }

}
