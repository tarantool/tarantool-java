package org.tarantool;

public class TarantoolClientConfig {

    /**
     * Username and password for authorization.
     */
    public String username;
    public String password;

    /**
     * Default ByteArrayOutputStream size  when make query serialization.
     */
    public int defaultRequestSize = 4096;

    /**
     * Initial size for map which holds futures of sent request.
     */
    public int predictedFutures = (int) ((1024 * 1024) / 0.75) + 1;


    public int writerThreadPriority = Thread.NORM_PRIORITY;

    public int readerThreadPriority = Thread.NORM_PRIORITY;


    /**
     * shared buffer is place where client collect requests when socket is busy on write.
     */
    public int sharedBufferSize = 8 * 1024 * 1024;
    /**
     * not put request into the shared buffer if request size is ge directWriteFactor * sharedBufferSize.
     */
    public double directWriteFactor = 0.5d;

    /**
     *  Use old call command https://github.com/tarantool/doc/issues/54,
     *  please ensure that you server supports new call command.
     */
    public boolean useNewCall = false;

    /**
     * Any blocking ops timeout.
     */
    public long initTimeoutMillis = 60 * 1000L;

    public long writeTimeoutMillis = 60 * 1000L;

}
