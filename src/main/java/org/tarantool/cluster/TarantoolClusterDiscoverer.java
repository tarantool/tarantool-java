package org.tarantool.cluster;

import java.util.Set;

/**
 * Discovery strategy to obtain a list of the cluster nodes.
 * This one can be used by {@link org.tarantool.RefreshableSocketProvider}
 * to provide support for fault tolerance property.
 *
 * @see org.tarantool.RefreshableSocketProvider
 */
public interface TarantoolClusterDiscoverer {

    /**
     * Gets nodes addresses in <code>host[:port]</code> format.
     *
     * @return list of the cluster nodes
     */
    Set<String> getInstances();

}
