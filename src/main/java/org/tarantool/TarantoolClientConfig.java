package org.tarantool;

public class TarantoolClientConfig {

    public static final int DEFAULT_OPERATION_EXPIRY_TIME_MILLIS = 1000;

    /**
     * Auth-related data.
     */
    public String username;
    public String password;

    /**
     * Default request size when make query serialization.
     */
    public int defaultRequestSize = 4096;

    /**
     * Initial capacity for the map which holds futures of sent request.
     */
    public int predictedFutures = (int) ((1024 * 1024) / 0.75) + 1;

    public int writerThreadPriority = Thread.NORM_PRIORITY;
    public int readerThreadPriority = Thread.NORM_PRIORITY;

    /**
     * Shared buffer size (place where client collects requests
     * when socket is busy on write).
     */
    public int sharedBufferSize = 8 * 1024 * 1024;

    /**
     * Factor to calculate a threshold whether request will be accommodated
     * in the shared buffer.
     * <p>
     * if request size exceeds <code>directWriteFactor * sharedBufferSize</code>
     * request is sent directly.
     */
    public double directWriteFactor = 0.5d;

    /**
     * Write operation timeout.
     */
    public long writeTimeoutMillis = 60 * 1000L;

    /**
     * Use old call command https://github.com/tarantool/doc/issues/54,
     * please ensure that you server supports new call command.
     */
    public boolean useNewCall = false;

    /**
     *  Max time to establish connection to the server
     *  and be completely configured (to have an {@code ALIVE} status).
     *
     * @see TarantoolClient#isAlive()
     */
    public long initTimeoutMillis = 60 * 1000L;

    /**
     * Connection timeout per attempt.
     * {@code 0} means no timeout.
     */
    public int connectionTimeout = 2 * 1000;

    /**
     * Total attempts number to connect to DB.
     * {@code 0} means unlimited attempts.
     */
    public int retryCount = 3;

    /**
     * Operation expiration period.
     */
    public int operationExpiryTimeMillis = DEFAULT_OPERATION_EXPIRY_TIME_MILLIS;

}
