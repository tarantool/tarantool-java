package org.tarantool.logging;

/**
 * Delegates a logger creation to SLF4J logger
 * adapter.
 * <p>
 * This class is not a part of public API.
 */
public class Slf4jLoggerProvider implements LoggerProvider {

    @Override
    public Logger getLogger(String name) {
        return new Slf4jLogger(name);
    }

}
