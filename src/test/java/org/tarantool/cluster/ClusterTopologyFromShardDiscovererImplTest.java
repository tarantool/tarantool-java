package org.tarantool.cluster;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.tarantool.TarantoolClusterClientConfig;
import org.tarantool.server.TarantoolNode;

import java.util.Collection;

class ClusterTopologyFromShardDiscovererImplTest {

    @DisplayName("Test that a list which describes the topology is fetched correctly")
    @Test
    void testListFetching() {
        TarantoolNode tarantoolNode = TarantoolNode.create("localhost:3301");
        TarantoolClusterClientConfig clientConfig = new TarantoolClusterClientConfig();

        clientConfig.username = "storage";
        clientConfig.password = "storage";

        Collection<TarantoolNode> tarantoolNodes =
                new ClusterTopologyFromShardDiscovererImpl(clientConfig)
                        .discoverTarantoolNodes(tarantoolNode, 5000);
        int i = 0;
    }
}