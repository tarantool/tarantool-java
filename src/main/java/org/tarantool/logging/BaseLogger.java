package org.tarantool.logging;

import java.util.function.Supplier;

abstract class BaseLogger implements Logger {

    public abstract void log(Level level, String text, Throwable error, Object... parameters);

    public void log(Level level, Throwable error, Supplier<String> message) {
        if (!isLoggable(level)) {
            return;
        }
        log(level, message.get(), error);
    }

    public abstract boolean isLoggable(Level level);

    @Override
    public void debug(String message) {
        log(Level.DEBUG, message, null);
    }

    @Override
    public void debug(String format, Object... params) {
        log(Level.DEBUG, format, null, params);
    }

    @Override
    public void debug(String message, Throwable throwable) {
        log(Level.DEBUG, message, throwable);
    }

    @Override
    public void debug(Supplier<String> message, Throwable throwable) {
        log(Level.DEBUG, throwable, message);
    }

    @Override
    public void error(String message) {
        log(Level.ERROR, message, null);
    }

    @Override
    public void error(String format, Object... params) {
        log(Level.ERROR, format, null, params);
    }

    @Override
    public void error(String message, Throwable throwable) {
        log(Level.ERROR, message, throwable);
    }

    @Override
    public void error(Supplier<String> message, Throwable throwable) {
        log(Level.ERROR, throwable, message);
    }

    @Override
    public void info(String message) {
        log(Level.INFO, message, null);
    }

    @Override
    public void info(String format, Object... params) {
        log(Level.INFO, format, null, params);
    }

    @Override
    public void info(String message, Throwable throwable) {
        log(Level.INFO, message, throwable);
    }

    @Override
    public void info(Supplier<String> message, Throwable throwable) {
        log(Level.INFO, throwable, message);
    }

    @Override
    public void trace(String message) {
        log(Level.TRACE, message, null);
    }

    @Override
    public void trace(String format, Object... params) {
        log(Level.TRACE, format, null, params);
    }

    @Override
    public void trace(String message, Throwable throwable) {
        log(Level.TRACE, message, throwable);
    }

    @Override
    public void trace(Supplier<String> message, Throwable throwable) {
        log(Level.TRACE, throwable, message);
    }

    @Override
    public void warn(String message) {
        log(Level.WARN, message, null);
    }

    @Override
    public void warn(String format, Object... params) {
        log(Level.WARN, format, null, params);
    }

    @Override
    public void warn(String message, Throwable throwable) {
        log(Level.WARN, message, throwable);
    }

    @Override
    public void warn(Supplier<String> message, Throwable throwable) {
        log(Level.WARN, throwable, message);
    }

    @Override
    public boolean isDebugEnabled() {
        return isLoggable(Level.DEBUG);
    }

    @Override
    public boolean isErrorEnabled() {
        return isLoggable(Level.ERROR);
    }

    @Override
    public boolean isInfoEnabled() {
        return isLoggable(Level.INFO);
    }

    @Override
    public boolean isTraceEnabled() {
        return isLoggable(Level.TRACE);
    }

    @Override
    public boolean isWarnEnabled() {
        return isLoggable(Level.WARN);
    }

}
