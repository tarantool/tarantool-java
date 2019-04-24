package org.tarantool;

public interface ConfigurableSocketChannelProvider extends SocketChannelProvider {

    int RETRY_NO_LIMIT = 0;
    int NO_TIMEOUT = 0;

    /**
     * Configures max count of retries.
     *
     * @param limit max attempts count
     */
    void setRetriesLimit(int limit);

    /**
     * Configures max time to establish
     * a connection per attempt.
     *
     * @param timeout connection timeout in millis
     */
    void setConnectionTimeout(int timeout);

}
