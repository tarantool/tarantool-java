package org.tarantool.cluster;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.tarantool.TarantoolClusterClientConfig;
import org.tarantool.server.TarantoolNodeInfo;

class ClusterTopologyFromShardDiscovererImplTest {

    @DisplayName("Test that a list which describes the topology is fetched correctly")
    @Test
    void testListFetching() {
        TarantoolNodeInfo tarantoolNodeInfo = TarantoolNodeInfo.create("localhost:3301");
        TarantoolClusterClientConfig clientConfig = new TarantoolClusterClientConfig();

        clientConfig.username = "storage";
        clientConfig.password = "storage";

//        Collection<TarantoolNodeInfo> tarantoolNodes =
//                new ClusterTopologyFromShardDiscovererImpl(clientConfig)
//                        .discoverTarantoolNodes(tarantoolNodeInfo, 5000);
        int i = 0;
    }
}