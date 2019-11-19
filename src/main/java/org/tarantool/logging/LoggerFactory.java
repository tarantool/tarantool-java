package org.tarantool.logging;

import java.util.Iterator;
import java.util.ServiceLoader;

/**
 * Basic service to load appropriate {@link LoggerProvider}
 * and obtain new loggers using loaded provider.
 * <p>
 * This class should be used as a start point to obtain the logger:
 * <pre> {@code
 * static final Logger LOGGER = LoggerFactory.getLogger(MyClass.java);
 * } </pre>
 * and then it can be used as a standard logger:
 * <pre> {@code
 * LOGGER.info("Service initialized");
 * } </pre>
 * <p>
 * There are four major attempts to load the logger provider:
 * <ol>
 * <li>
 * Use a runtime system property {@literal org.tarantool.logging.provider}.
 * Possible values are 'slf4j' or 'jdk'. (for instance,
 * {@literal -Dorg.tarantool.logging.provider=slf4j}).
 * </li>
 * <li>
 * Use SPI mechanism to find the proper {@link LoggerProvider}.
 * </li>
 * <li>
 * Check the classpath in attempt to discover one of supported logger
 * backends.
 * </li>
 * <li>
 * Use JUL implementation if none of above attempts are successful.
 * </li>
 * </ol>
 * <p>
 * This class is not a part of public API.
 */
public class LoggerFactory {

    private static final String LOGGING_PROVIDER_KEY = "org.tarantool.logging.provider";
    private static final String LOGGING_PROVIDER_JDK = "jdk";
    private static final String LOGGING_PROVIDER_SLF4J = "slf4j";

    private static LoggerProvider loggerProvider = loadLoggerProvider();

    private static LoggerProvider loadLoggerProvider() {
        // use a runtime property to determine a logger provider name
        try {
            String providerName = System.getProperty(LOGGING_PROVIDER_KEY);
            if (providerName != null) {
                if (providerName.equalsIgnoreCase(LOGGING_PROVIDER_JDK)) {
                    return tryLoadJdkProvider();
                } else if (providerName.equalsIgnoreCase(LOGGING_PROVIDER_SLF4J)) {
                    return tryLoadSlf4jProvider();
                }
            }
        } catch (Throwable ignored) {
            // no-op
        }

        // use SPI to pick a proper logger provider
        try {
            ServiceLoader<LoggerProvider> serviceLoader = ServiceLoader.load(LoggerProvider.class);
            Iterator<LoggerProvider> iterator = serviceLoader.iterator();
            while (loggerProvider == null && iterator.hasNext()) {
                try {
                    return iterator.next();
                } catch (Throwable ignored) {
                    // no-op
                }
            }
        } catch (Throwable ignored) {
            // no-op
        }

        // check slf4j-logback existence in the class path
        try {
            Class.forName("ch.qos.logback.classic.Logger", false, LoggerFactory.class.getClassLoader());
            return tryLoadSlf4jProvider();
        } catch (Throwable ignored) {
            // no-op
        }

        // use JUL logger as a default logger
        return tryLoadJdkProvider();
    }


    private static LoggerProvider tryLoadJdkProvider() {
        return new JdkLoggerProvider();
    }

    private static LoggerProvider tryLoadSlf4jProvider() {
        return new Slf4jLoggerProvider();
    }

    private LoggerFactory() {
    }

    /**
     * Gets a logger provided by this factory.
     *
     * @param name logger name
     *
     * @return obtained logger
     */
    public static Logger getLogger(String name) {
        return loggerProvider.getLogger(name);
    }

    /**
     * Gets a logger provided by this factory.
     *
     * @param type target class
     *
     * @return obtained logger
     */
    public static Logger getLogger(Class<?> type) {
        return loggerProvider.getLogger(type.getName());
    }

}
