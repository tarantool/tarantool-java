package org.tarantool.logging;

import java.util.logging.LogRecord;

/**
 * Adapter to JUL logger backend.
 * <p>
 * This class is not a part of public API.
 *
 * @see java.util.logging
 */
class JdkLogger extends BaseLogger {

    private final java.util.logging.Logger delegate;

    public JdkLogger(String loggerName) {
        this.delegate = java.util.logging.Logger.getLogger(loggerName);
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

        java.util.logging.Level julLevel = translate(level);
        LogRecord record = new LogRecord(julLevel, text);
        record.setLoggerName(delegate.getName());
        record.setResourceBundle(delegate.getResourceBundle());
        record.setResourceBundleName(delegate.getResourceBundleName());
        if (error != null) {
            record.setThrown(error);
        }
        record.setParameters(parameters);
        delegate.log(record);
    }

    @Override
    public boolean isLoggable(Level level) {
        return delegate.isLoggable(translate(level));
    }

    private java.util.logging.Level translate(Level level) {
        switch (level) {
        case ERROR:
            return java.util.logging.Level.SEVERE;
        case WARN:
            return java.util.logging.Level.WARNING;
        case INFO:
            return java.util.logging.Level.INFO;
        case DEBUG:
            return java.util.logging.Level.FINE;
        case TRACE:
            return java.util.logging.Level.FINEST;
        default:
            return java.util.logging.Level.ALL;
        }
    }

}
