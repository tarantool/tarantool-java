package org.tarantool.cluster;

import org.tarantool.server.TarantoolInstanceInfo;

import java.util.List;

public interface ClusterTopologyDiscoverer {
    List<TarantoolInstanceInfo> discoverTarantoolInstances(Integer infoHostConnectionTimeout);
}