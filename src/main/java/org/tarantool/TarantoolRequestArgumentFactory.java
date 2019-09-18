package org.tarantool;

import java.util.Objects;
import java.util.function.Supplier;

/**
 * Request argument factory.
 *
 * @see TarantoolRequestArgument
 */
public class TarantoolRequestArgumentFactory {

    private TarantoolRequestArgumentFactory() {
    }

    public static TarantoolRequestArgument value(Object value) {
        return new SimpleArgument(value);
    }

    public static TarantoolRequestArgument cacheLookupValue(Supplier<Object> supplier) {
        return new LookupArgument(supplier);
    }

    /**
     * Simple wrapper that holds the original value.
     */
    private static class SimpleArgument implements TarantoolRequestArgument {

        private Object value;

        SimpleArgument(Object value) {
            Objects.requireNonNull(value);
            this.value = value;
        }

        @Override
        public boolean isSerializable() {
            return true;
        }

        @Override
        public Object getValue() {
            return value;
        }

    }

    /**
     * Wrapper that evaluates the value each time
     * it is requested.
     * <p>
     * It works like a function, where {@code argument = f(key)}.
     */
    private static class LookupArgument implements TarantoolRequestArgument {

        Supplier<Object> lookup;

        LookupArgument(Supplier<Object> lookup) {
            this.lookup = Objects.requireNonNull(lookup);
        }

        @Override
        public boolean isSerializable() {
            try {
                lookup.get();
            } catch (Exception ignored) {
                return false;
            }
            return true;
        }

        @Override
        public synchronized Object getValue() {
            return lookup.get();
        }

    }

}
