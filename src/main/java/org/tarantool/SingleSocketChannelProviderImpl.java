package org.tarantool;

import org.tarantool.util.StringUtils;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.channels.SocketChannel;

/**
 * Simple provider that produces a single connection.
 * To be used with {@link TarantoolClientImpl}.
 */
public class SingleSocketChannelProviderImpl extends BaseSocketChannelProvider {

    private InetSocketAddress address;

    /**
     *  Creates a simple provider.
     *
     * @param address instance address
     */
    public SingleSocketChannelProviderImpl(String address) {
        setAddress(address);
    }

    @Override
    public SocketAddress getAddress() {
        return address;
    }

    @Override
    protected SocketChannel makeAttempt(int retryNumber, Throwable lastError) throws IOException {
        if (areRetriesExhausted(retryNumber)) {
            throw new CommunicationException("Connection retries exceeded.", lastError);
        }
        return openChannel(address);
    }

    /**
     * Provides a decision on whether retries limit is hit.
     *
     * @param retryNumber current count of retries.
     *
     * @return {@code true} if retries are exhausted.
     */
    private boolean areRetriesExhausted(int retryNumber) {
        int limit = getRetriesLimit();
        if (limit < 1) {
            return false;
        }
        return retryNumber >= limit;
    }

    public void setAddress(String address) {
        if (StringUtils.isBlank(address)) {
            throw new IllegalArgumentException("address must not be empty");
        }

        this.address = parseAddress(address);
    }

}
