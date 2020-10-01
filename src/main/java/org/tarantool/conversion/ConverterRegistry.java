package org.tarantool.conversion;

/**
 * Entry point for internal type conversions.
 * It uses set of registered converters to perform
 * transformations between types.
 */
public interface ConverterRegistry {

    /**
     * Checks whether an available converter exists between
     * specified types or not.
     *
     * @param from source type
     * @param to   target type
     *
     * @return {@literal true} if two types are convertible
     */
    boolean isConvertible(Class<?> from, Class<?> to);

    /**
     * Converts the source value to target type value using
     * an appropriate registered converter.
     *
     * @param object     source object
     * @param targetType destination type class
     * @param <B>        target type
     *
     * @return converted value
     *
     * @throws NotConvertibleValueException if conversion is not supported or failed
     */
    <B> B convert(Object object, Class<B> targetType);

    /**
     * Registers a new converter between two types.
     * It replaces the previous one converter if latter
     * was registered before.
     *
     * @param from      source type class
     * @param to        target type class
     * @param converter converter from source to target types
     * @param <A>       source type
     * @param <B>       target type
     */
    <A, B> void addConverter(Class<A> from, Class<B> to, Converter<? super A, ? extends B> converter);

    /**
     * Unregisters previously added converter from source
     * to target types.
     *
     * @param from source type
     * @param to   target type
     *
     * @return {@literal true} if a converter was found and unregistered
     */
    boolean removeConvertible(Class<?> from, Class<?> to);

}
