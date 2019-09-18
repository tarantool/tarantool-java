package org.tarantool;

/**
 * Provides a set of typical operations with data in Tarantool.
 *
 * @param <T> represents space/index identifiers (not used anymore)
 * @param <O> represents tuple keys and/or tuples
 * @param <P> represents tuples and/or update operations
 * @param <R> represents an operation result
 */
public interface TarantoolClientOps<T, O, P, R> {
    R select(Integer space, Integer index, O key, int offset, int limit, int iterator);

    R select(String space, String index, O key, int offset, int limit, int iterator);

    R select(Integer space, Integer index, O key, int offset, int limit, Iterator iterator);

    R select(String space, String index, O key, int offset, int limit, Iterator iterator);

    R insert(Integer space, O tuple);

    R insert(String space, O tuple);

    R replace(Integer space, O tuple);

    R replace(String space, O tuple);

    R update(Integer space, O key, P... tuple);

    R update(String space, O key, P... tuple);

    R upsert(Integer space, O key, O defTuple, P... ops);

    R upsert(String space, O key, O defTuple, P... ops);

    R delete(Integer space, O key);

    R delete(String space, O key);

    R call(String function, Object... args);

    R eval(String expression, Object... args);

    void ping();

    void close();
}
