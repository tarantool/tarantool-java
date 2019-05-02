package org.tarantool;

/**
 * Defines a common SQL operations for DQL or DML/DDL.
 *
 * @param <Tuple>  type of rows to be passed as parameters
 * @param <Update> type of result for DML/DDL operations
 * @param <Result> type of result for DQL operations
 */
public interface TarantoolSQLOps<Tuple, Update, Result> {

    /**
     * Executes a DQL query, typically {@code SELECT} query,
     * and returns an obtained result set.
     *
     * @param sql  query
     * @param bind arguments to be bound with query parameters
     *
     * @return result of query
     */
    Result query(String sql, Tuple... bind);

    /**
     * Executes a DML/DDL query using optional parameters.
     *
     * @param sql  query
     * @param bind arguments to be bound with query parameters
     *
     * @return result of query
     */
    Update update(String sql, Tuple... bind);

}
