package org.tarantool;

import org.tarantool.cluster.ClusterTopologyDiscoverer;
import org.tarantool.cluster.ClusterTopologyFromShardDiscovererImpl;
import org.tarantool.server.*;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

import static org.tarantool.TarantoolClientImpl.StateHelper.CLOSED;

/**
 * Basic implementation of a client that may work with the cluster
 * of tarantool instances in fault-tolerant way.
 *
 * Failed operations will be retried once connection is re-established
 * unless the configured expiration time is over.
 */
public class TarantoolClusterClient extends TarantoolClientImpl {
    /* Need some execution context to retry writes. */
    private Executor executor;

    /* Collection of operations to be retried. */
    private ConcurrentHashMap<Long, ExpirableOp<?>> retries = new ConcurrentHashMap<Long, ExpirableOp<?>>();

    private final Collection<TarantoolInstanceInfo> slaveHosts;

    private final TarantoolInstanceInfo infoHost;
    private final Integer infoHostConnectionTimeout;
    private final ClusterTopologyDiscoverer topologyDiscoverer;
    /**
     * @param config Configuration.
     */
    public TarantoolClusterClient(TarantoolClusterClientConfig config) {
//        this(config, new RoundRobinSocketProviderImpl(config.slaveHosts).setTimeout(config.operationExpiryTimeMillis));
        this(config, new RoundRobinNodeCommunicationProvider(config.slaveHosts,
                config.username, config.password, config.operationExpiryTimeMillis));
    }

    /**
     * @param provider Socket channel provider.
     * @param config Configuration.
     */
    public TarantoolClusterClient(TarantoolClusterClientConfig config, NodeCommunicationProvider provider) {
        super(provider, config);

        this.executor = config.executor == null ?
            Executors.newSingleThreadExecutor() : config.executor;
        this.infoHost = TarantoolInstanceInfo.create(config.infoHost, config.username, config.password);

        this.infoHostConnectionTimeout = config.infoHostConnectionTimeout;
        this.topologyDiscoverer = new ClusterTopologyFromShardDiscovererImpl(config);

        slaveHosts = topologyDiscoverer.discoverTarantoolNodes(this.infoHost, infoHostConnectionTimeout);
    }

    /**
     * @param infoNode a node from which a topology of the cluster is discovered.
     * @throws CommunicationException in case of communication with {@code infoNode} exception
     * @throws IllegalArgumentException in case when the info node returned invalid address
     */
    private Collection<TarantoolInstanceInfo> refreshServerList(TarantoolInstanceInfo infoNode) {
        List<TarantoolInstanceInfo> newServerList = topologyDiscoverer
                .discoverTarantoolNodes(infoNode, infoHostConnectionTimeout);

        writeLock.lock();
//        todo add a read lock
        try {

            RoundRobinNodeCommunicationProvider cp = (RoundRobinNodeCommunicationProvider) this.communicationProvider;

/*            TarantoolInstanceInfo currentNode = cp.getCurrentNode();

            int sameNodeIndex = newServerList.indexOf(currentNode);
            if (sameNodeIndex != -1) {
                Collections.swap(newServerList, 0, sameNodeIndex);
                cp.setNodes(newServerList);
            } else {
                cp.setNodes(newServerList);
                die("The server list have been changed.", null);
                //todo
            }*/

            cp.updateNodes(newServerList);


        } finally {
            writeLock.unlock();
        }

        return newServerList;
    }

    @Override
    protected boolean isDead(CompletableFuture<?> q) {
        if ((state.getState() & CLOSED) != 0) {
            q.completeExceptionally(new CommunicationException("Connection is dead", thumbstone));
            return true;
        }
        Exception err = thumbstone;
        if (err != null) {
            return checkFail(q, err);
        }
        return false;
    }

    @Override
    protected CompletableFuture<?> doExec(Code code, Object[] args) {
        validateArgs(args);
        long sid = syncId.incrementAndGet();
        CompletableFuture<?> q = makeFuture(sid, code, args);

        if (isDead(q)) {
            return q;
        }
        futures.put(sid, q);
        if (isDead(q)) {
            futures.remove(sid);
            return q;
        }
        try {
            write(code, sid, null, args);
        } catch (Exception e) {
            futures.remove(sid);
            fail(q, e);
        }
        return q;
    }

    @Override
    protected void fail(CompletableFuture<?> q, Exception e) {
        checkFail(q, e);
    }

    protected boolean checkFail(CompletableFuture<?> q, Exception e) {
        assert q instanceof ExpirableOp<?>;
        if (!isTransientError(e) || ((ExpirableOp<?>)q).hasExpired(System.currentTimeMillis())) {
            q.completeExceptionally(e);
            return true;
        } else {
            assert retries != null;
            retries.put(((ExpirableOp<?>) q).getId(), (ExpirableOp<?>)q);
            return false;
        }
    }

    @Override
    protected void close(Exception e) {
        super.close(e);

        if (retries == null) {
            // May happen within constructor.
            return;
        }

        for (ExpirableOp<?> op : retries.values()) {
            op.completeExceptionally(e);
        }
    }

    protected boolean isTransientError(Exception e) {
        if (e instanceof CommunicationException) {
            return true;
        }
        if (e instanceof TarantoolException) {
            return ((TarantoolException)e).isTransient();
        }
        return false;
    }

    protected CompletableFuture<?> makeFuture(long id, Code code, Object...args) {
        int expireTime = ((TarantoolClusterClientConfig) config).operationExpiryTimeMillis;
        return new ExpirableOp(id, expireTime, code, args);
    }

    /**
     * Reconnect is over, schedule retries.
     */
    @Override
    protected void onReconnect() {
        if (retries == null || executor == null) {
            // First call is before the constructor finished. Skip it.
            return;
        }
        Collection<ExpirableOp<?>> futsToRetry = new ArrayList<ExpirableOp<?>>(retries.values());
        retries.clear();
        long now = System.currentTimeMillis();
        for (final ExpirableOp<?> fut : futsToRetry) {
            if (!fut.hasExpired(now)) {
                executor.execute(new Runnable() {
                    @Override
                    public void run() {
                        futures.put(fut.getId(), fut);
                        try {
                            write(fut.getCode(), fut.getId(), null, fut.getArgs());
                        } catch (Exception e) {
                            futures.remove(fut.getId());
                            fail(fut, e);
                        }
                    }
                });
            }
        }
    }

    /**
     * Holds operation code and arguments for retry.
     */
    private class ExpirableOp<V> extends CompletableFuture<V> {
        /** Moment in time when operation is not considered for retry. */
        final private long deadline;

        /**
         * A task identifier used in {@link TarantoolClientImpl#futures}.
         */
        final private long id;

        /**
         * Tarantool binary protocol operation code.
         */
        final private Code code;

        /** Arguments of operation. */
        final private Object[] args;

        /**
         *
         * @param id Sync.
         * @param expireTime Expiration time (relative) in ms.
         * @param code Tarantool operation code.
         * @param args Operation arguments.
         */
        ExpirableOp(long id, int expireTime, Code code, Object...args) {
            this.id = id;
            this.deadline = System.currentTimeMillis() + expireTime;
            this.code = code;
            this.args = args;
        }

        boolean hasExpired(long now) {
            return now > deadline;
        }

        public long getId() {
            return id;
        }

        public Code getCode() {
            return code;
        }

        public Object[] getArgs() {
            return args;
        }
    }
}
