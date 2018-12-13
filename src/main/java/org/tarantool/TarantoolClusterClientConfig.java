package org.tarantool;

import java.util.concurrent.Executor;

/**
 * Configuration for the {@link TarantoolClusterClient}.
 */
public class TarantoolClusterClientConfig extends TarantoolClientConfig {
    /* Amount of time (in milliseconds) the operation is eligible for retry. */
    public int operationExpiryTimeMillis = 500;

    /* Executor service that will be used as a thread of execution to retry writes. */
    public Executor executor = null;
}
