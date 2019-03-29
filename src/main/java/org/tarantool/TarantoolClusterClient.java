package org.tarantool;

import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

import static org.tarantool.TarantoolClientImpl.StateHelper.CLOSED;

/**
 * Basic implementation of a client that may work with the cluster
 * of tarantool instances in fault-tolerant way.
 * <p>
 * Failed operations will be retried once connection is re-established
 * unless the configured expiration time is over.
 */
public class TarantoolClusterClient extends TarantoolClientImpl {
    /* Need some execution context to retry writes. */
    private Executor executor;

    /* Collection of operations to be retried. */
    private ConcurrentHashMap<Long, ExpirableOp<?>> retries = new ConcurrentHashMap<Long, ExpirableOp<?>>();

    /**
     * @param config Configuration.
     * @param addrs  Array of addresses in the form of [host]:[port].
     */
    public TarantoolClusterClient(TarantoolClusterClientConfig config, String... addrs) {
        this(config, new RoundRobinSocketProviderImpl(addrs).setTimeout(config.operationExpiryTimeMillis));
    }

    /**
     * @param provider Socket channel provider.
     * @param config   Configuration.
     */
    public TarantoolClusterClient(TarantoolClusterClientConfig config, SocketChannelProvider provider) {
        super(provider, config);

        this.executor = config.executor == null ?
                Executors.newSingleThreadExecutor() : config.executor;
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
        ExpirableOp<?> future = makeFuture(sid, code, args);

        if (isDead(future)) {
            return future;
        }
        futures.put(sid, future);
        if (isDead(future)) {
            futures.remove(sid);
            return future;
        }
        try {
            write(code, sid, null, args);
        } catch (Exception e) {
            futures.remove(sid);
            fail(future, e);
        }
        return future;
    }

    @Override
    protected void fail(CompletableFuture<?> q, Exception e) {
        checkFail(q, e);
    }

    protected boolean checkFail(CompletableFuture<?> q, Exception e) {
        assert q instanceof ExpirableOp<?>;
        if (!isTransientError(e) || ((ExpirableOp<?>) q).hasExpired(System.currentTimeMillis())) {
            q.completeExceptionally(e);
            return true;
        } else {
            assert retries != null;
            retries.put(((ExpirableOp<?>) q).getId(), (ExpirableOp<?>) q);
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
            return ((TarantoolException) e).isTransient();
        }
        return false;
    }

    private ExpirableOp<?> makeFuture(long id, Code code, Object... args) {
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
        Collection<ExpirableOp<?>> futuresToRetry = new ArrayList<ExpirableOp<?>>(retries.values());
        retries.clear();
        long now = System.currentTimeMillis();
        for (final ExpirableOp<?> future : futuresToRetry) {
            if (!future.hasExpired(now)) {
                executor.execute(new Runnable() {
                    @Override
                    public void run() {
                        futures.put(future.getId(), future);
                        try {
                            write(future.getCode(), future.getId(), null, future.getArgs());
                        } catch (Exception e) {
                            futures.remove(future.getId());
                            fail(future, e);
                        }
                    }
                });
            }
        }
    }

    /**
     * Holds operation code and arguments for retry.
     */
    private class ExpirableOp<V> extends TarantoolOp<V> {

        /**
         * Moment in time when operation is not considered for retry.
         */
        final private long deadline;

        /**
         * A task identifier used in {@link TarantoolClientImpl#futures}.
         */
        final private long id;

        /**
         * Arguments of operation.
         */
        final private Object[] args;

        /**
         * @param id         Sync.
         * @param expireTime Expiration time (relative) in ms.
         * @param code       Tarantool operation code.
         * @param args       Operation arguments.
         */
        ExpirableOp(long id, int expireTime, Code code, Object... args) {
            super(code);
            this.id = id;
            this.deadline = System.currentTimeMillis() + expireTime;
            this.args = args;
        }

        boolean hasExpired(long now) {
            return now > deadline;
        }

        public long getId() {
            return id;
        }

        public Object[] getArgs() {
            return args;
        }

    }
}
