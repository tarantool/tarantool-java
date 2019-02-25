package org.tarantool.cluster;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.tarantool.TarantoolClusterClientConfig;
import org.tarantool.server.TarantoolInstanceInfo;

class ClusterTopologyFromShardDiscovererImplTest {

    @DisplayName("Test that a list which describes the topology is fetched correctly")
    @Test
    void testListFetching() {
        //todo
        TarantoolInstanceInfo tarantoolInstanceInfo = TarantoolInstanceInfo
                .create("localhost:3301", "testUsername", "testPassword");
        TarantoolClusterClientConfig clientConfig = new TarantoolClusterClientConfig();

        clientConfig.username = "storage";
        clientConfig.password = "storage";

//        Collection<TarantoolInstanceInfo> tarantoolNodes =
//                new ClusterTopologyFromShardDiscovererImpl(clientConfig)
//                        .discoverTarantoolNodes(tarantoolInstanceInfo, 5000);
        int i = 0;
    }
}