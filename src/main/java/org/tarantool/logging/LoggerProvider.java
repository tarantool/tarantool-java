package org.tarantool.logging;

/**
 * Provides loggers depended on logger implementations.
 * This interface is also used as SPI and its implementations
 * can be discovered when {@link LoggerFactory} is loaded.
 * <p>
 * This class is not a part of public API.
 */
public interface LoggerProvider {

    /**
     * Gets a logger by its name.
     *
     * @param name logger name
     *
     * @return constructed logger
     */
    Logger getLogger(final String name);

}
