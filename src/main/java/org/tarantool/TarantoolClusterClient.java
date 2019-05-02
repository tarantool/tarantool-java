package org.tarantool;

import org.tarantool.cluster.TarantoolClusterDiscoverer;
import org.tarantool.cluster.TarantoolClusterStoredFunctionDiscoverer;
import org.tarantool.protocol.TarantoolPacket;
import org.tarantool.util.StringUtils;

import java.io.IOException;
import java.net.SocketAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.StampedLock;

/**
 * Basic implementation of a client that may work with the cluster
 * of tarantool instances in fault-tolerant way.
 * <p>
 * Failed operations will be retried once connection is re-established
 * unless the configured expiration time is over.
 */
public class TarantoolClusterClient extends TarantoolClientImpl {

    /**
     * Need some execution context to retry writes.
     */
    private Executor executor;

    /**
     * Discovery activity.
     */
    private ScheduledExecutorService instancesDiscoveryExecutor;
    private Runnable instancesDiscovererTask;
    private StampedLock discoveryLock = new StampedLock();

    /**
     * Collection of operations to be retried.
     */
    private ConcurrentHashMap<Long, TarantoolOp<?>> retries = new ConcurrentHashMap<>();

    /**
     * Constructs a new cluster client.
     *
     * @param config    Configuration.
     * @param addresses Array of addresses in the form of host[:port].
     */
    public TarantoolClusterClient(TarantoolClusterClientConfig config, String... addresses) {
        this(config, makeClusterSocketProvider(addresses));
    }

    /**
     * Constructs a new cluster client.
     *
     * @param provider Socket channel provider.
     * @param config   Configuration.
     */
    public TarantoolClusterClient(TarantoolClusterClientConfig config, SocketChannelProvider provider) {
        super(provider, config);

        this.executor = config.executor == null
            ? Executors.newSingleThreadExecutor()
            : config.executor;

        if (StringUtils.isNotBlank(config.clusterDiscoveryEntryFunction)) {
            this.instancesDiscovererTask =
                createDiscoveryTask(new TarantoolClusterStoredFunctionDiscoverer(config, this));
            this.instancesDiscoveryExecutor
                = Executors.newSingleThreadScheduledExecutor(new TarantoolThreadDaemonFactory("tarantoolDiscoverer"));
            int delay = config.clusterDiscoveryDelayMillis > 0
                ? config.clusterDiscoveryDelayMillis
                : TarantoolClusterClientConfig.DEFAULT_CLUSTER_DISCOVERY_DELAY_MILLIS;

            // todo: it's better to start a job later (out of ctor)
            this.instancesDiscoveryExecutor.scheduleWithFixedDelay(
                this.instancesDiscovererTask,
                0,
                delay,
                TimeUnit.MILLISECONDS
            );
        }
    }

    @Override
    protected boolean isDead(TarantoolOp<?> future) {
        if ((state.getState() & StateHelper.CLOSED) != 0) {
            future.completeExceptionally(new CommunicationException("Connection is dead", thumbstone));
            return true;
        }
        Exception err = thumbstone;
        if (err != null) {
            return checkFail(future, err);
        }
        return false;
    }

    @Override
    protected TarantoolOp<?> doExec(long timeoutMillis, Code code, Object[] args) {
        validateArgs(args);
        long sid = syncId.incrementAndGet();
        TarantoolOp<?> future = makeNewOperation(timeoutMillis, sid, code, args);
        return registerOperation(future);
    }

    /**
     * Registers a new async operation which will be resolved later.
     * Registration is discovery-aware in term of synchronization and
     * it may be blocked util the discovery finishes its work.
     *
     * @param future operation to be performed
     *
     * @return registered operation
     */
    private TarantoolOp<?> registerOperation(TarantoolOp<?> future) {
        long stamp = discoveryLock.readLock();
        try {
            if (isDead(future)) {
                return future;
            }
            futures.put(future.getId(), future);
            if (isDead(future)) {
                futures.remove(future.getId());
                return future;
            }

            try {
                write(future.getCode(), future.getId(), null, future.getArgs());
            } catch (Exception e) {
                futures.remove(future.getId());
                fail(future, e);
            }

            return future;
        } finally {
            discoveryLock.unlock(stamp);
        }
    }

    @Override
    protected void fail(TarantoolOp<?> future, Exception e) {
        checkFail(future, e);
    }

    protected boolean checkFail(TarantoolOp<?> future, Exception e) {
        if (!isTransientError(e)) {
            future.completeExceptionally(e);
            return true;
        } else {
            assert retries != null;
            retries.put(future.getId(), future);
            return false;
        }
    }

    @Override
    protected void close(Exception e) {
        super.close(e);

        if (instancesDiscoveryExecutor != null) {
            instancesDiscoveryExecutor.shutdownNow();
        }

        if (retries == null) {
            // May happen within constructor.
            return;
        }

        for (TarantoolOp<?> op : retries.values()) {
            op.completeExceptionally(e);
        }
    }

    protected boolean isTransientError(Exception e) {
        if (e instanceof CommunicationException) {
            return true;
        }
        if (e instanceof TarantoolException) {
            return ((TarantoolException) e).isTransient();
        }
        return false;
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
        Collection<TarantoolOp<?>> futuresToRetry = new ArrayList<>(retries.values());
        retries.clear();
        for (final TarantoolOp<?> future : futuresToRetry) {
            if (!future.isDone()) {
                executor.execute(() -> registerOperation(future));
            }
        }
    }

    @Override
    protected void complete(TarantoolPacket packet, TarantoolOp<?> future) {
        super.complete(packet, future);
        RefreshableSocketProvider provider = getRefreshableSocketProvider();
        if (provider != null) {
            renewConnectionIfRequired(provider.getAddresses());
        }
    }

    protected void onInstancesRefreshed(Set<String> instances) {
        RefreshableSocketProvider provider = getRefreshableSocketProvider();
        if (provider != null) {
            provider.refreshAddresses(instances);
            renewConnectionIfRequired(provider.getAddresses());
        }
    }

    private RefreshableSocketProvider getRefreshableSocketProvider() {
        return socketProvider instanceof RefreshableSocketProvider
            ? (RefreshableSocketProvider) socketProvider
            : null;
    }

    private void renewConnectionIfRequired(Collection<SocketAddress> addresses) {
        if (pendingResponsesCount.get() > 0 || !isAlive()) {
            return;
        }
        SocketAddress addressInUse = getCurrentAddressOrNull();
        if (!(addressInUse == null || addresses.contains(addressInUse))) {
            long stamp = discoveryLock.tryWriteLock();
            if (!discoveryLock.validate(stamp)) {
                return;
            }
            try {
                if (pendingResponsesCount.get() == 0) {
                    stopIO();
                }
            } finally {
                discoveryLock.unlock(stamp);
            }
        }
    }

    private SocketAddress getCurrentAddressOrNull() {
        try {
            return channel.getRemoteAddress();
        } catch (IOException ignored) {
            return null;
        }
    }

    public void refreshInstances() {
        if (instancesDiscovererTask != null) {
            instancesDiscovererTask.run();
        }
    }

    private static RoundRobinSocketProviderImpl makeClusterSocketProvider(String[] addresses) {
        return new RoundRobinSocketProviderImpl(addresses);
    }

    private Runnable createDiscoveryTask(TarantoolClusterDiscoverer serviceDiscoverer) {
        return new Runnable() {

            private Set<String> lastInstances;

            @Override
            public synchronized void run() {
                try {
                    Set<String> freshInstances = serviceDiscoverer.getInstances();
                    if (!(freshInstances.isEmpty() || Objects.equals(lastInstances, freshInstances))) {
                        lastInstances = freshInstances;
                        onInstancesRefreshed(lastInstances);
                    }
                } catch (Exception ignored) {
                    // no-op
                }
            }
        };
    }

}
