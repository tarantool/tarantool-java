package org.tarantool.conversion;

/**
 * Raised if an attempt of conversion is failed.
 */
public class NotConvertibleValueException extends RuntimeException {

    private final Class<?> from;
    private final Class<?> to;

    public NotConvertibleValueException(Class<?> from, Class<?> to) {
        super("Could not convert types " + from + " -> " + to + ". Unsupported conversion.");
        this.from = from;
        this.to = to;
    }

    public NotConvertibleValueException(Class<?> from, Class<?> to, Throwable cause) {
        super("Could not convert types " + from + " -> " + to, cause);
        this.from = from;
        this.to = to;
    }

    public Class<?> getFrom() {
        return from;
    }

    public Class<?> getTo() {
        return to;
    }

}
