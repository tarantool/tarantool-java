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

    /**
     * Array of addresses in the form of [host]:[port].
     */
    public String[] slaveHosts;

    /**
     * Address of a tarantool instance form which a cluster host list can be discovered.
     */
    public String infoHost;

    /**
     * Name of a function that called on info host instance to fetch the list of
     * tarantool cluster instances
     */
    public String infoFunctionName;

    /**
     * timeout of connecting to a info host
     */
    public int infoHostConnectionTimeout = 500;

}
