package org.tarantool;

import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

public class TarantoolThreadDaemonFactory implements ThreadFactory {

    private static final AtomicInteger POOL_NUMBER = new AtomicInteger(1);
    private final AtomicInteger threadNumber = new AtomicInteger(1);
    private final String namePrefix;

    public TarantoolThreadDaemonFactory(String namePrefix) {
        this.namePrefix = namePrefix + "-" + POOL_NUMBER.incrementAndGet() + "-thread-";
    }

    @Override
    public Thread newThread(Runnable runnable) {
        Thread thread = new Thread(runnable, namePrefix + threadNumber.incrementAndGet());
        thread.setDaemon(true);

        return thread;
    }
}
