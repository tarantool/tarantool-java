package org.tarantool;

import java.util.concurrent.Executor;

/**
 * Configuration for the {@link TarantoolClusterClient}.
 */
public class TarantoolClusterClientConfig extends TarantoolClientConfig {

    public static final int DEFAULT_CLUSTER_DISCOVERY_DELAY_MILLIS = 60_000;

    /**
     * Executor that will be used as a thread of
     * execution to retry writes.
     */
    public Executor executor;

    /**
     * Gets a name of the stored function to be used
     * to fetch list of instances.
     */
    public String clusterDiscoveryEntryFunction;

    /**
     * Scan period for refreshing a new list of instances.
     */
    public int clusterDiscoveryDelayMillis = DEFAULT_CLUSTER_DISCOVERY_DELAY_MILLIS;

}
