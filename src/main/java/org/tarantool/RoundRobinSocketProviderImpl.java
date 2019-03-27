package org.tarantool;

import java.net.InetSocketAddress;
import java.util.Arrays;

/**
 * Basic reconnection strategy that changes addresses in a round-robin fashion.
 * To be used with {@link TarantoolClientImpl}.
 */
public class RoundRobinSocketProviderImpl extends ReconnectingSocketProvider {
    /** Server addresses as configured. */
    private final String[] addrs;
    /** Socket addresses. */
    private final InetSocketAddress[] sockAddrs;
    /** Current position within {@link #sockAddrs} array. */
    private int pos;

    /**
     * Constructs an instance.
     *
     * @param addrs Array of addresses in a form of [host]:[port].
     */
    public RoundRobinSocketProviderImpl(String... addrs) {
        if (addrs == null || addrs.length == 0)
            throw new IllegalArgumentException("addrs is null or empty.");

        this.addrs = Arrays.copyOf(addrs, addrs.length);

        sockAddrs = new InetSocketAddress[this.addrs.length];

        for (int i = 0; i < this.addrs.length; i++) {
            sockAddrs[i] = parseAddress(this.addrs[i]);
        }
    }

    /**
     * @return Configured addresses in a form of [host]:[port].
     */
    public String[] getAddresses() {
        return this.addrs;
    }

    /**
     * @return Number of configured addresses.
     */
    protected int getAddressCount() {
        return sockAddrs.length;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int getRetriesLimit() {
        return super.getRetriesLimit() * getAddressCount();
    }

    /**
     * @return Socket address to use for the next reconnection attempt.
     */
    protected InetSocketAddress getSocketAddress() {
        InetSocketAddress res = sockAddrs[pos];
        pos = (pos + 1) % sockAddrs.length;
        return res;
    }

    /**
     * Parse a string address in the form of [host]:[port]
     * and builds a socket address.
     *
     * @param addr Server address.
     * @return Socket address.
     */
    protected InetSocketAddress parseAddress(String addr) {
        int idx = addr.indexOf(':');
        String host = (idx < 0) ? addr : addr.substring(0, idx);
        int port = (idx < 0) ? 3301 : Integer.parseInt(addr.substring(idx + 1));
        return new InetSocketAddress(host, port);
    }
}
