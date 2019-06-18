package org.tarantool.cluster;

import org.tarantool.TarantoolClient;
import org.tarantool.TarantoolClientOps;
import org.tarantool.TarantoolClusterClientConfig;
import org.tarantool.util.StringUtils;

import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * A cluster nodes discoverer based on calling a predefined function
 * which returns list of nodes.
 *
 * The function has to have no arguments and return list of
 * the strings which follow <code>host[:port]</code> format
 */
public class TarantoolClusterStoredFunctionDiscoverer implements TarantoolClusterDiscoverer {

    private TarantoolClient client;
    private String entryFunction;

    public TarantoolClusterStoredFunctionDiscoverer(TarantoolClusterClientConfig clientConfig, TarantoolClient client) {
        this.client = client;
        this.entryFunction = clientConfig.clusterDiscoveryEntryFunction;
    }

    @Override
    public Set<String> getInstances() {
        TarantoolClientOps<Integer, List<?>, Object, List<?>> syncOperations = client.syncOps();

        List<?> list = syncOperations.call(entryFunction);
        // discoverer expects a single array result from the function now;
        // in order to protect this contract the discoverer does a strict
        // validation against the data returned;
        // this strict-mode allows us to extend the contract in a non-breaking
        // way for old clients just reserve an extra return value in
        // terms of Lua multi-result support.;
        return checkAndFilterAddresses(list);
    }

    /**
     * Check whether the result follows the contract or not.
     * The contract is a mandatory <b>single array of strings</b>.
     *
     * The simplified format for each string is host[:port].
     *
     * @param result result to be validated
     */
    private Set<String> checkAndFilterAddresses(List<?> result) {
        if (result == null || result.isEmpty()) {
            throw new IllegalDiscoveryFunctionResult("Discovery function returned no data");
        }
        if (!(result.get(0) instanceof List)) {
            throw new IllegalDiscoveryFunctionResult("The first value must be an array of strings");
        }

        return ((List<Object>) result.get(0)).stream()
            .filter(item -> item instanceof String)
            .map(Object::toString)
            .filter(s -> !StringUtils.isBlank(s))
            .filter(this::isAddress)
            .collect(Collectors.toCollection(LinkedHashSet::new));
    }

    /**
     * Checks that address matches host[:port] format.
     *
     * @param address to be checked
     * @return true if address follows the format
     */
    private boolean isAddress(String address) {
        if (address.endsWith(":")) {
            return false;
        }
        String[] addressParts = address.split(":");
        if (addressParts.length > 2) {
            return false;
        }
        if (addressParts.length == 2) {
            try {
                int port = Integer.parseInt(addressParts[1]);
                return (port > 0 && port < 65536);
            } catch (NumberFormatException e) {
                return false;
            }
        }
        return true;
    }

}
