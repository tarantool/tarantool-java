package org.tarantool.logging;

/**
 * Delegates a logger creation to JUL logger adapter.
 * <p>
 * This class is not a part of public API.
 */
public class JdkLoggerProvider implements LoggerProvider {

    @Override
    public Logger getLogger(String name) {
        return new JdkLogger(name);
    }

}
