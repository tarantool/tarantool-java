package org.tarantool.jdbc;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;

/**
 * Tarantool specific connection extensions.
 */
public interface TarantoolConnection extends Connection {

    /**
     * Executes SQL query.
     *
     * @param timeout query timeout
     * @param query   query holder
     *
     * @return wrapper that holds processed sql results
     *
     * @throws SQLException if errors occur while the query is being performed.
     *                      {@link java.sql.SQLTimeoutException} is raised when execution time exceeds the timeout
     */
    SQLResultHolder execute(long timeout, SQLQueryHolder query) throws SQLException;

    /**
     * Executes number of update queries. The given timeout will
     * be applied to each query from the list.
     *
     * @param timeout query timeout
     * @param queries list of queries to be executed in a batch
     *
     * @return wrapper that holds processed sql results
     *
     * @throws SQLException if errors occur while the query is being performed.
     *                      {@link java.sql.SQLTimeoutException} is raised when execution time exceeds the timeout
     */
    SQLBatchResultHolder executeBatch(long timeout, List<SQLQueryHolder> queries)
        throws SQLException;

}
