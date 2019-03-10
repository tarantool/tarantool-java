package org.tarantool.cluster;

import org.tarantool.TarantoolClientImpl;
import org.tarantool.TarantoolClientOps;
import org.tarantool.TarantoolClusterClientConfig;
import org.tarantool.server.TarantoolInstanceInfo;

import java.util.List;
import java.util.stream.Collectors;

public class ClusterTopologyFromFunctionDiscovererImpl implements ClusterTopologyDiscoverer {

    private final TarantoolClusterClientConfig clientConfig;

    private final TarantoolInstanceInfo infoNode;
    private final String functionName;

    public ClusterTopologyFromFunctionDiscovererImpl(TarantoolClusterClientConfig clientConfig) {
        this.clientConfig = clientConfig;
        if (clientConfig.infoFunctionName == null) {
            throw new IllegalArgumentException("infoFuntionName in the config cannot be null");
        }
        if (clientConfig.infoHost == null) {
            throw new IllegalArgumentException("infoHost in the config cannot be null");
        }

        functionName = clientConfig.infoFunctionName;
        this.infoNode = TarantoolInstanceInfo.create(
                clientConfig.infoHost, clientConfig.username, clientConfig.password);
    }

    @Override
    public List<TarantoolInstanceInfo> discoverTarantoolInstances(Integer infoHostConnectionTimeout) {

        TarantoolClientOps<Integer, List<?>, Object, List<?>> syncOps =
                new TarantoolClientImpl(infoNode.getSocketAddress(), clientConfig)
                        .syncOps();

        List<?> list = syncOps
                .call(functionName);

        List<Object> funcResult = (List<Object>) list.get(0);
        return funcResult.stream()
                .map(addr -> TarantoolInstanceInfo.create(
                        parseReplicaUri(addr.toString()), clientConfig.username, clientConfig.password))
                .collect(Collectors.toList());
    }

    private String parseReplicaUri(String uri) {
        String[] split = uri.split("@");
        if (split.length == 2) {
            return split[1];
        } else {
            return split[0];
        }
    }
}
