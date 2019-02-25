package org.tarantool.cluster;

import org.tarantool.server.TarantoolNodeInfo;

import java.util.List;

public interface ClusterTopologyDiscoverer {
    List<TarantoolNodeInfo> discoverTarantoolNodes(TarantoolNodeInfo infoNode, Integer infoHostConnectionTimeout);
}
