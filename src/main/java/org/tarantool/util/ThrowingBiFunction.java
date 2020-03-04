package org.tarantool.util;

/**
 * Represents a function that accepts two arguments and
 * produces a result or throws an exception.
 *
 * @param <T> type of the first argument to the function
 * @param <U> type of the second argument to the function
 * @param <R> type of the result of the function
 * @param <E> type of the exception in case of error
 */
@FunctionalInterface
public interface ThrowingBiFunction<T, U, R, E extends Exception> {

    /**
     * Applies this function to the given arguments.
     *
     * @param argument1 first argument
     * @param argument2 second argument
     *
     * @return function result
     *
     * @throws E if any error occurs
     */
    R apply(T argument1, U argument2) throws E;

}
