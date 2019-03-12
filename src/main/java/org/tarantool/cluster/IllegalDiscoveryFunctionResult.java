package org.tarantool.cluster;

/**
 *  Raised when {@link TarantoolClusterStoredFunctionDiscoverer} validates
 *  a function result as unsupported.
 */
public class IllegalDiscoveryFunctionResult extends RuntimeException {

    public IllegalDiscoveryFunctionResult(String message) {
        super(message);
    }

}
