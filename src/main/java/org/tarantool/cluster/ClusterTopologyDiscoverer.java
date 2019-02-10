package org.tarantool.cluster;

import org.tarantool.server.TarantoolNode;

import java.util.List;

public interface ClusterTopologyDiscoverer {
    List<TarantoolNode> discoverTarantoolNodes(TarantoolNode infoNode, Integer infoHostConnectionTimeout);
}
