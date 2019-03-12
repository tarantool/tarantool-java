package org.tarantool;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

/**
 * Basic reconnection strategy that changes addresses in a round-robin fashion.
 * To be used with {@link TarantoolClientImpl}.
 */
public class RoundRobinSocketProviderImpl extends BaseSocketChannelProvider implements RefreshableSocketProvider {

    private static final int UNSET_POSITION = -1;

    /**
     * Socket addresses pool.
     */
    private final List<InetSocketAddress> socketAddresses = new ArrayList<>();

    /**
     * Current position within {@link #socketAddresses} list.
     * <p>
     * It is {@link #UNSET_POSITION} when no addresses from
     * the {@link #socketAddresses} pool have been processed yet.
     * <p>
     * When this provider receives new addresses it tries
     * to look for a new position for the last used address or
     * sets the position to {@link #UNSET_POSITION} otherwise.
     *
     * @see #getLastObtainedAddress()
     * @see #refreshAddresses(Collection)
     */
    private AtomicInteger currentPosition = new AtomicInteger(UNSET_POSITION);

    /**
     * Address list lock for a thread-safe access to it
     * when a refresh operation occurs.
     *
     * @see RefreshableSocketProvider#refreshAddresses(Collection)
     */
    private ReadWriteLock addressListLock = new ReentrantReadWriteLock();

    /**
     * Constructs an instance.
     *
     * @param addresses optional array of addresses in a form of host[:port]
     *
     * @throws IllegalArgumentException if addresses aren't provided
     */
    public RoundRobinSocketProviderImpl(String... addresses) {
        updateAddressList(Arrays.asList(addresses));
    }

    private void updateAddressList(Collection<String> addresses) {
        if (addresses == null || addresses.isEmpty()) {
            throw new IllegalArgumentException("At least one address must be provided");
        }
        Lock writeLock = addressListLock.writeLock();
        writeLock.lock();
        try {
            InetSocketAddress lastAddress = getLastObtainedAddress();
            socketAddresses.clear();
            addresses.stream()
                .map(this::parseAddress)
                .collect(Collectors.toCollection(() -> socketAddresses));
            if (lastAddress != null) {
                int recoveredPosition = socketAddresses.indexOf(lastAddress);
                currentPosition.set(recoveredPosition);
            } else {
                currentPosition.set(UNSET_POSITION);
            }
        } finally {
            writeLock.unlock();
        }
    }

    /**
     * Gets parsed and resolved internet addresses.
     *
     * @return socket addresses
     */
    public List<SocketAddress> getAddresses() {
        Lock readLock = addressListLock.readLock();
        readLock.lock();
        try {
            return Collections.unmodifiableList(this.socketAddresses);
        } finally {
            readLock.unlock();
        }
    }

    /**
     * Gets last used address from the pool if it exists.
     *
     * @return last obtained address or <code>null</code>
     *     if {@link #currentPosition} has {@link #UNSET_POSITION} value
     */
    protected InetSocketAddress getLastObtainedAddress() {
        Lock readLock = addressListLock.readLock();
        readLock.lock();
        try {
            int index = currentPosition.get();
            return index != UNSET_POSITION ? socketAddresses.get(index) : null;
        } finally {
            readLock.unlock();
        }
    }

    @Override
    protected InetSocketAddress getAddress(int retryNumber, Throwable lastError) throws IOException {
        return getNextSocketAddress();
    }

    /**
     * Gets size of addresses pool.
     *
     * @return Number of configured addresses.
     */
    protected int getAddressCount() {
        Lock readLock = addressListLock.readLock();
        readLock.lock();
        try {
            return socketAddresses.size();
        } finally {
            readLock.unlock();
        }
    }

    /**
     * Gets next address from the pool to be used to connect.
     *
     * @return Socket address to use for the next reconnection attempt
     */
    protected InetSocketAddress getNextSocketAddress() {
        Lock readLock = addressListLock.readLock();
        readLock.lock();
        try {
            int position = currentPosition.updateAndGet(i -> (i + 1) % socketAddresses.size());
            return socketAddresses.get(position);
        } finally {
            readLock.unlock();
        }
    }

    /**
     * Update addresses pool by new list.
     *
     * @param addresses list of addresses to be applied
     *
     * @throws IllegalArgumentException if addresses list is empty
     */
    public void refreshAddresses(Collection<String> addresses) {
        updateAddressList(addresses);
    }

}
