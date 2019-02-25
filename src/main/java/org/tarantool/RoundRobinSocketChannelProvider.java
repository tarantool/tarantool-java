package org.tarantool;

import org.tarantool.server.BinaryProtoUtils;
import org.tarantool.server.TarantoolBinaryPackage;
import org.tarantool.server.TarantoolInstanceConnection;
import org.tarantool.server.TarantoolInstanceInfo;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.List;

public class RoundRobinSocketChannelProvider implements SocketChannelProvider {

    /** Timeout to establish socket connection with an individual server. */
    private int timeout; // 0 is infinite.

    /** Limit of retries. */
    private int retriesLimit = -1; // No-limit.


    private TarantoolInstanceInfo[] nodes;
    private TarantoolInstanceInfo currentNode;

    private int pos = 0;

    public RoundRobinSocketChannelProvider(String[] slaveHosts, String username, String password) {
        if (slaveHosts == null || slaveHosts.length < 1) {
            throw new IllegalArgumentException("slave hosts is null ot empty");
        }

        updateNodes(slaveHosts, username, password);
    }

    private void updateNodes(String[] slaveHosts, String username, String password) {
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

    /** {@inheritDoc} */
    @Override
    public SocketChannel getNext() {
        int attempts = getAddressCount();
        long deadline = System.currentTimeMillis() + timeout * attempts;
        while (!Thread.currentThread().isInterrupted()) {
            SocketChannel channel = null;
            try {
                TarantoolInstanceInfo tarantoolInstanceInfo = getNextNode();


                TarantoolInstanceConnection nodeConnection = TarantoolInstanceConnection.connect(tarantoolInstanceInfo);

                channel = nodeConnection.getChannel();

                return channel;
            } catch (IOException e) {
                if (channel != null) {
                    try {
                        channel.close();
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


    private final TarantoolInstanceInfo tarantoolInstanceInfo;

    private TarantoolInstanceConnection nodeConnection;

    @Override
    public void connect() {
        nodeConnection = TarantoolInstanceConnection.connect(tarantoolInstanceInfo);
    }


    public void writeBuffer(ByteBuffer byteBuffer) throws IOException {
        SocketChannel channel2Write = getChannel();
        BinaryProtoUtils.writeFully(channel2Write, byteBuffer);
    }

    public TarantoolBinaryPackage readPackage() throws IOException {
        SocketChannel channel2Read = getChannel();
        return BinaryProtoUtils.readPacket(channel2Read);
    }

    @Override
    public SocketChannel getChannel() {
        if (nodeConnection == null) {
            throw new IllegalStateException("Not initialized");
        }
        return nodeConnection.getChannel();
    }
}
