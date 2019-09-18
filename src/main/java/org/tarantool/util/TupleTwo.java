package org.tarantool.util;

/**
 * Two places tuple.
 */
public class TupleTwo<T, U> {

    private final T first;
    private final U second;

    TupleTwo(T first, U second) {
        this.first = first;
        this.second = second;
    }

    public static <T, U> TupleTwo<T, U> of(T first, U second) {
        return new TupleTwo<>(first, second);
    }

    public T getFirst() {
        return first;
    }

    public U getSecond() {
        return second;
    }

}
