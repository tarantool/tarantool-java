package org.tarantool;

public class TarantoolClientConfig {

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
     * Use old call command https://github.com/tarantool/doc/issues/54,
     * please ensure that you server supports new call command.
     */
    public boolean useNewCall = false;

    /**
     * Limits for synchronous operations.
     */
    public long initTimeoutMillis = 60 * 1000L;
    public long writeTimeoutMillis = 60 * 1000L;

}
