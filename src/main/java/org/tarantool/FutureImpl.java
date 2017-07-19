package org.tarantool;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.AbstractQueuedSynchronizer;


public class FutureImpl<V> extends AbstractQueuedSynchronizer implements Future<V> {
    protected final long id;
    protected Code code;
    protected V value;
    protected Exception error;

    public FutureImpl(long id, Code code) {
        this.id = id;
        this.code = code;
        setState(1);
    }

    public FutureImpl(long id, Exception error) {
        this.id = id;
        this.error = error;
    }

    public void setValue(V v) {
        value = v;
        releaseShared(1);
    }

    public void setError(Exception e) {
        error = e;
        releaseShared(1);
    }


    @Override
    protected boolean tryReleaseShared(int releases) {
        // Decrement count; signal when transition to zero
        for (;;) {
            int c = getState();
            if (c == 0)
                return false;
            int nextc = c-1;
            if (compareAndSetState(c, nextc))
                return nextc == 0;
        }
    }

    @Override
    public boolean cancel(boolean mayInterruptIfRunning) {
        return false;
    }

    @Override
    public boolean isCancelled() {
        return false;
    }

    @Override
    public boolean isDone() {
        return getState() == 0;
    }

    @Override
    public V get() throws InterruptedException, ExecutionException {
        acquireSharedInterruptibly(1);
        return value();
    }

    @Override
    protected int tryAcquireShared(int acquires) {
        return (getState() == 0) ? 1 : -1;
    }

    private V value() throws ExecutionException {
        if (error != null) {
            throw new ExecutionException(error);
        }
        return value;
    }

    @Override
    public V get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
        if(!tryAcquireSharedNanos(1, unit.toNanos(timeout))) {
            throw new TimeoutException();
        }
        return value();
    }

    public Long getId() {
        return id;
    }

    public Code getCode() {
        return code;
    }
}
