package org.tarantool.logging;

import java.text.MessageFormat;

/**
 * Adapter to SLF4J and Logback logger backends.
 * <p>
 * This class is not a part of public API.
 */
class Slf4jLogger extends BaseLogger {

    private final org.slf4j.Logger delegate;

    public Slf4jLogger(String loggerName) {
        delegate = org.slf4j.LoggerFactory.getLogger(loggerName);
    }

    @Override
    public String getName() {
        return delegate.getName();
    }

    @Override
    public void log(Level level, String text, Throwable error, Object... parameters) {
        if (!isLoggable(level)) {
            return;
        }

        String message = (parameters == null || parameters.length == 0) ? text : MessageFormat.format(text, parameters);
        switch (level) {
        case ERROR:
            delegate.error(message, error);
            break;
        case WARN:
            delegate.warn(message, error);
            break;
        case INFO:
            delegate.info(message, error);
            break;
        case DEBUG:
            delegate.debug(message, error);
            break;
        case TRACE:
            delegate.trace(message, error);
            break;
        default:
            break;
        }
    }

    @Override
    public boolean isLoggable(Level level) {
        switch (level) {
        case ERROR:
            return delegate.isErrorEnabled();
        case WARN:
            return delegate.isWarnEnabled();
        case INFO:
            return delegate.isInfoEnabled();
        case DEBUG:
            return delegate.isDebugEnabled();
        case TRACE:
            return delegate.isTraceEnabled();
        default:
            return true;
        }
    }

}
