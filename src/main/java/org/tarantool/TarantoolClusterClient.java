package org.tarantool;

import org.tarantool.cluster.TarantoolClusterDiscoverer;
import org.tarantool.cluster.TarantoolClusterStoredFunctionDiscoverer;
import org.tarantool.logging.Logger;
import org.tarantool.logging.LoggerFactory;
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

    private static final Logger LOGGER = LoggerFactory.getLogger(TarantoolClusterClient.class);

    /**
     * Need some execution context to retry writes.
     */
    private Executor executor;

    /**
     * Discovery activity.
     */
    private Runnable instancesDiscovererTask;
    private StampedLock discoveryLock = new StampedLock();

    /**
     * Collection of operations to be retried.
     */
    private ConcurrentHashMap<Long, TarantoolOperation> retries = new ConcurrentHashMap<>();

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
            int delay = config.clusterDiscoveryDelayMillis > 0
                ? config.clusterDiscoveryDelayMillis
                : TarantoolClusterClientConfig.DEFAULT_CLUSTER_DISCOVERY_DELAY_MILLIS;

            // todo: it's better to start a job later (out of ctor)
            this.workExecutor.scheduleWithFixedDelay(
                this.instancesDiscovererTask,
                0,
                delay,
                TimeUnit.MILLISECONDS
            );
        }
    }

    @Override
    protected boolean isDead(TarantoolOperation operation) {
        if ((state.getState() & StateHelper.CLOSED) != 0) {
            operation.getResult().completeExceptionally(new CommunicationException("Connection is dead", thumbstone));
            return true;
        }
        Exception err = thumbstone;
        if (err != null) {
            return checkFail(operation, err);
        }
        return false;
    }

    /**
     * Registers a new async operation which will be resolved later.
     * Registration is discovery-aware in term of synchronization and
     * it may be blocked util the discovery finishes its work.
     *
     * @param operation operation to be performed
     *
     * @return registered operation
     */
    @Override
    protected TarantoolOperation registerOperation(TarantoolOperation operation) {
        long stamp = discoveryLock.readLock();
        try {
            return super.registerOperation(operation);
        } finally {
            discoveryLock.unlock(stamp);
        }
    }

    @Override
    protected void fail(TarantoolOperation operation, Exception cause) {
        checkFail(operation, cause);
    }

    protected boolean checkFail(TarantoolOperation operation, Exception cause) {
        if (!isTransientError(cause)) {
            operation.getResult().completeExceptionally(cause);
            return true;
        } else {
            assert retries != null;
            retries.put(operation.getId(), operation);
            LOGGER.trace("Request {0} was delayed because of {1}", operation, cause);
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

        for (TarantoolOperation operation : retries.values()) {
            operation.getResult().completeExceptionally(e);
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
        Collection<TarantoolOperation> delayed = new ArrayList<>(retries.values());
        Collection<TarantoolOperation> reissued = new ArrayList<>(retries.size());
        retries.clear();
        for (final TarantoolOperation operation : delayed) {
            if (!operation.getResult().isDone()) {
                operation.setSentSchemaId(schemaMeta.getSchemaVersion());
                executor.execute(() -> registerOperation(operation));
                reissued.add(operation);
            }
        }
        for (final TarantoolOperation operation : reissued) {
            LOGGER.trace("{0} was re-issued after reconnection", operation);
        }
    }

    @Override
    protected void complete(TarantoolPacket packet, TarantoolOperation operation) {
        super.complete(packet, operation);
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
