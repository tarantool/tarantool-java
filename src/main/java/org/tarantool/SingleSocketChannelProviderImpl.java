package org.tarantool;

import org.tarantool.util.StringUtils;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;

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

    public SocketAddress getAddress() {
        return address;
    }

    @Override
    protected InetSocketAddress getAddress(int retryNumber, Throwable lastError) throws IOException {
        return address;
    }

    public void setAddress(String address) {
        if (StringUtils.isBlank(address)) {
            throw new IllegalArgumentException("address must not be empty");
        }

        this.address = parseAddress(address);
    }

}
