package org.tarantool;

import org.tarantool.server.TarantoolInstanceConnection;
import org.tarantool.server.TarantoolInstanceInfo;

import java.io.IOException;
import java.net.InetSocketAddress;

public class SingleInstanceConnectionProvider implements InstanceConnectionProvider {

    private final TarantoolInstanceInfo tarantoolInstanceInfo;

    private TarantoolInstanceConnection nodeConnection;

    public SingleInstanceConnectionProvider(InetSocketAddress socketAddress, String username, String password) {
        this.tarantoolInstanceInfo = TarantoolInstanceInfo.create(socketAddress, username, password);
    }

    public SingleInstanceConnectionProvider(String address, String username, String password) {
        this.tarantoolInstanceInfo = TarantoolInstanceInfo.create(address, username, password);
    }

    @Override
    public TarantoolInstanceConnection connect() throws IOException {
        nodeConnection = TarantoolInstanceConnection.connect(tarantoolInstanceInfo);
        return nodeConnection;
    }

    @Override
    public String getDescription() {
        if (nodeConnection != null) {
            return nodeConnection.getNodeInfo().getSocketAddress().toString();
        } else {
            return "Unconnected. Node " + tarantoolInstanceInfo.getSocketAddress().toString();
        }
    }
}
