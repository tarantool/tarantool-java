package org.tarantool.conversion;

/**
 * Conversion unit used to transform
 * values from one type to another.
 *
 * @param <A> source type
 * @param <B> target type
 */
public interface Converter<A, B> {

    /**
     * Converts a value of a source type to
     * target.
     *
     * @param source value of the source type
     *
     * @return converted value of target type
     *
     * @throws NotConvertibleValueException if error occurs while conversation
     */
    B convert(A source);

}
