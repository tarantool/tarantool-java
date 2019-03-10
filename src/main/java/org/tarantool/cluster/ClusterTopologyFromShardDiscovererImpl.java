package org.tarantool.cluster;

import org.tarantool.TarantoolClientImpl;
import org.tarantool.TarantoolClusterClientConfig;
import org.tarantool.server.TarantoolInstanceInfo;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class ClusterTopologyFromShardDiscovererImpl implements ClusterTopologyDiscoverer {

    private final TarantoolClusterClientConfig clientConfig;
    private final TarantoolInstanceInfo infoNode;

    private static final String DEFAULT_TOPOLOGY_DISCOVER_CALL = "require('vshard').storage.internal.current_cfg";

    public ClusterTopologyFromShardDiscovererImpl(TarantoolClusterClientConfig clientConfig) {
        this.clientConfig = clientConfig;
        this.infoNode = TarantoolInstanceInfo.create(
                clientConfig.infoHost, clientConfig.username, clientConfig.password);
    }

    @Override
    public List<TarantoolInstanceInfo> discoverTarantoolInstances(Integer infoHostConnectionTimeout) {

        List<?> list = new TarantoolClientImpl(infoNode.getSocketAddress(), clientConfig)
                .syncOps()
                .call(DEFAULT_TOPOLOGY_DISCOVER_CALL);

        Map funcResult = (Map) ((List) list.get(0)).get(0);

        Map shardHash2DescriptionMap = (Map) getValue(funcResult, "sharding");

        List<TarantoolInstanceInfo> result = new ArrayList<>();


        for (Object shardHash2Description : shardHash2DescriptionMap.entrySet()) {

            Map replicas = (Map) getValue(((Map.Entry) shardHash2Description).getValue(), "replicas");

            for (Object replica : replicas.entrySet()) {
                Object replicaUri = getValue(((Map.Entry) replica).getValue(), "uri");

                result.add(TarantoolInstanceInfo.create(
                        parseReplicaUri(replicaUri.toString()), clientConfig.username, clientConfig.password));
            }
        }

        return result;
    }

    private String parseReplicaUri(String uri) {
        String[] split = uri.split("@");
        if (split.length == 2) {
            return split[1];
        } else {
            return split[0];
        }
    }

    private Object getValue(Object map, String key) {
        if (!(map instanceof Map)) {
            throw new IllegalArgumentException("Argument 'map' is not instance of Map but " + map.getClass());
        }

        return ((Map) map).get(key);
    }
}
