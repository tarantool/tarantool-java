package org.tarantool;

import org.tarantool.server.BinaryProtoUtils;
import org.tarantool.server.TarantoolBinaryPackage;
import org.tarantool.server.TarantoolInstanceConnection;
import org.tarantool.server.TarantoolInstanceInfo;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.*;
import java.util.stream.*;

public class RoundRobinNodeCommunicationProvider implements NodeCommunicationProvider {

    /** Timeout to establish socket connection with an individual server. */
    private final int timeout; // 0 is infinite.

    /** Limit of retries. */
    private int retriesLimit = -1; // No-limit.


    private TarantoolInstanceInfo[] nodes;
    private TarantoolInstanceConnection currentConnection;

    private int pos = 0;

    public RoundRobinNodeCommunicationProvider(String[] slaveHosts, String username, String password, int timeout) {
        this.timeout = timeout;
        if (slaveHosts == null || slaveHosts.length < 1) {
            throw new IllegalArgumentException("slave hosts is null ot empty");
        }

        setNodes(slaveHosts, username, password);
    }

    private void setNodes(String[] slaveHosts, String username, String password) {
        //todo add read-write lock
        nodes = new TarantoolInstanceInfo[slaveHosts.length];
        for (int i = 0; i < slaveHosts.length; i++) {
            String slaveHostAddress = slaveHosts[i];
            nodes[i] = TarantoolInstanceInfo.create(slaveHostAddress, username, password);
        }

        pos = 0;
    }

    public void updateNodes(List<TarantoolInstanceInfo> slaveHosts) {
        if (slaveHosts == null) {
            throw new IllegalArgumentException("slaveHosts can not be null");
        }
        //todo add read-write lock

        this.nodes = (TarantoolInstanceInfo[]) slaveHosts.toArray();

        pos = 0;
    }


    /**
     * @return Non-empty list of round-robined nodes
     */
    public TarantoolInstanceInfo[] getNodes() {
        return nodes;
    }

    /** {@inheritDoc} */
    public TarantoolInstanceConnection connectNextNode() {
        int attempts = getAddressCount();
        long deadline = System.currentTimeMillis() + timeout * attempts;
        while (!Thread.currentThread().isInterrupted()) {
            TarantoolInstanceConnection connection = null;
            try {
                TarantoolInstanceInfo tarantoolInstanceInfo = getNextNode();
                connection = TarantoolInstanceConnection.connect(tarantoolInstanceInfo);
                return connection;
            } catch (IOException e) {
                if (connection != null) {
                    try {
                        connection.close();
                    } catch (IOException ignored) {
                        // No-op.
                    }
                }
                long now = System.currentTimeMillis();
                if (deadline <= now) {
                    throw new CommunicationException("Connection time out.", e);
                }
                if (--attempts == 0) {
                    // Tried all addresses without any lack, but still have time.
                    attempts = getAddressCount();
                    try {
                        Thread.sleep((deadline - now) / attempts);
                    } catch (InterruptedException ignored) {
                        Thread.currentThread().interrupt();
                    }
                }
            }
        }
        throw new CommunicationException("Thread interrupted.", new InterruptedException());
    }

    /**
     * @return Socket address to use for the next reconnection attempt.
     */
    protected TarantoolInstanceInfo getNextNode() {
        TarantoolInstanceInfo res = nodes[pos];
        pos = (pos + 1) % nodes.length;
        return res;
    }


    /**
     * @return Number of configured addresses.
     */
    protected int getAddressCount() {
        return nodes.length;
    }


    @Override
    public void connect() {
        currentConnection = connectNextNode();
    }

    public void writeBuffer(ByteBuffer byteBuffer) throws IOException {
        SocketChannel channel2Write = getChannel();
        BinaryProtoUtils.writeFully(channel2Write, byteBuffer);
    }

    public TarantoolBinaryPackage readPackage() throws IOException {
        SocketChannel channel2Read = getChannel();
        return BinaryProtoUtils.readPacket(channel2Read);
    }

    private SocketChannel getChannel() {
        if (currentConnection == null) {
            throw new IllegalStateException("Not initialized");
        }
        return currentConnection.getChannel();
    }

    @Override
    public String getDescription() {
        if (currentConnection != null) {
            return currentConnection.getNodeInfo().getSocketAddress().toString();
        } else {
            return "Unconnected. Available nodes [" + Arrays.stream(nodes)
                    .map(instanceInfo -> instanceInfo.getSocketAddress().toString())
                    .collect(Collectors.joining(", "));
        }
    }
}
