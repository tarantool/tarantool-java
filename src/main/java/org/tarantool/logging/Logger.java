package org.tarantool.logging;

import java.util.function.Supplier;

/**
 * Minimal logger contract to perform internal
 * logging.
 * <p>
 * This class is not a part of public API.
 */
public interface Logger {

    enum Level {
        ERROR,
        WARN,
        INFO,
        DEBUG,
        TRACE
    }

    String getName();

    boolean isErrorEnabled();

    void error(final String message);

    void error(final String format, final Object... params);

    void error(final String message, Throwable throwable);

    void error(final Supplier<String> message, Throwable throwable);

    boolean isWarnEnabled();

    void warn(final String message);

    void warn(final String format, final Object... params);

    void warn(final String message, Throwable throwable);

    void warn(final Supplier<String> message, Throwable throwable);

    boolean isInfoEnabled();

    void info(final String message);

    void info(final String format, final Object... params);

    void info(final String message, Throwable throwable);

    void info(final Supplier<String> message, Throwable throwable);

    boolean isDebugEnabled();

    void debug(final String message);

    void debug(final String format, final Object... params);

    void debug(final String message, Throwable throwable);

    void debug(final Supplier<String> message, Throwable throwable);

    boolean isTraceEnabled();

    void trace(final String message);

    void trace(final String format, final Object... params);

    void trace(final String message, Throwable throwable);

    void trace(final Supplier<String> message, Throwable throwable);

}
