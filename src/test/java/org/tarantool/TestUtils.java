package org.tarantool;

import java.io.IOException;
import java.math.BigInteger;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketAddress;
import java.nio.channels.SocketChannel;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class TestUtils {

    public static TarantoolClient makeTestClient(TarantoolClientConfig config, int restartTimeout) {
        return new TarantoolClientImpl(
            new TestSocketChannelProvider(TarantoolTestHelper.HOST, TarantoolTestHelper.PORT, restartTimeout),
            config
        );
    }

    public static TarantoolConnection openConnection(String host, int port, String username, String password) {
        Socket socket = new Socket();
        try {
            socket.connect(new InetSocketAddress(host, port));
        } catch (IOException e) {
            throw new RuntimeException("Test failed due to invalid environment.", e);
        }
        try {
            return new TarantoolConnection(username, password, socket);
        } catch (Exception e) {
            try {
                socket.close();
            } catch (IOException ignored) {
                // No-op.
            }
            throw new RuntimeException(e);
        }
    }

    public static String makeDiscoveryFunction(String functionName, Collection<?> addresses) {
        String functionResult = addresses.stream()
            .map(address -> "'" + address + "'")
            .collect(Collectors.joining(",", "{", "}"));
        return makeDiscoveryFunction(functionName, functionResult);
    }

    public static String makeDiscoveryFunction(String functionName, Object result) {
        return makeDiscoveryFunction(functionName, result.toString());
    }

    public static String makeDiscoveryFunction(String functionName, String body) {
        return "function " + functionName + "() return " + body + " end";
    }

    static final String replicationInfoRequest = "return " +
        "box.info.id, " +
        "box.info.lsn, " +
        "box.info.replication";

    public static String makeReplicationString(String user, String pass, String... addrs) {
        StringBuilder sb = new StringBuilder();
        for (int idx = 0; idx < addrs.length; idx++) {
            if (sb.length() > 0) {
                sb.append(';');
            }
            sb.append(user);
            sb.append(':');
            sb.append(pass);
            sb.append('@');
            sb.append(addrs[idx]);
        }
        return sb.toString();
    }

    public static Map<String, String> makeInstanceEnv(int port, int consolePort) {
        Map<String, String> env = new HashMap<String, String>();
        env.put("LISTEN", Integer.toString(port));
        env.put("ADMIN", Integer.toString(consolePort));
        return env;
    }

    public static Map<String, String> makeInstanceEnv(int port,
                                                      int consolePort,
                                                      String replicationConfig,
                                                      double replicationTimeout) {
        Map<String, String> env = makeInstanceEnv(port, consolePort);
        env.put("MASTER", replicationConfig);
        env.put("REPLICATION_TIMEOUT", Double.toString(replicationTimeout));
        return env;
    }

    /**
     * See waitReplication(TarantoolClientImpl client, int timeout).
     */
    protected static void waitReplication(TarantoolConsole console, int timeout) {
        long deadline = System.currentTimeMillis() + timeout;
        for (; ; ) {
            List<?> v = console.evalList(replicationInfoRequest);

            if (parseAndCheckReplicationStatus(v)) {
                return;
            }

            if (deadline < System.currentTimeMillis()) {
                throw new RuntimeException("Test failure: timeout waiting for replication.");
            }

            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }

    /**
     * Wait until all replicas will be in sync with master's log.
     * <p>
     * It is useful to wait until the last data modification performed on
     * **that** instance will be applied on its replicas. It does not take care
     * to modifications performed on instances, which are master's of that one.
     */
    public static void waitReplication(TarantoolClientImpl client, int timeout) {
        long deadline = System.currentTimeMillis() + timeout;
        for (; ; ) {
            List<?> v;
            try {
                v = client.syncOps().eval(replicationInfoRequest);
            } catch (TarantoolException ignored) {
                continue;
            }

            if (parseAndCheckReplicationStatus(v)) {
                return;
            }

            if (deadline < System.currentTimeMillis()) {
                throw new RuntimeException("Test failure: timeout waiting for replication.");
            }

            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }

    public static String toHex(byte[] bytes) {
        StringBuilder sb = new StringBuilder(2 * bytes.length);
        for (byte b : bytes) {
            sb.append(String.format("%02x", b));
        }
        return sb.toString();
    }

    public static byte[] fromHex(String hex) {
        assert hex.length() % 2 == 0;
        byte[] data = new byte[hex.length() / 2];
        for (int i = 0; i < data.length; i++) {
            data[i] = Integer.decode("0x" + hex.charAt(i * 2) + hex.charAt(i * 2 + 1)).byteValue();
        }
        return data;
    }

    private static boolean parseAndCheckReplicationStatus(List data) {
        if (data == null || data.size() != 3) {
            throw new IllegalStateException("Unexpected format of replication status.");
        }

        Number masterId = ensureType(Number.class, data.get(0));
        Number masterLsn = ensureType(Number.class, data.get(1));
        Map<?, ?> replInfo = ensureTypeOrNull(Map.class, data.get(2));

        if (replInfo == null || replInfo.size() < 2) {
            return false;
        }

        for (Object info : replInfo.values()) {
            Map<?, ?> replItems = ensureTypeOrNull(Map.class, info);

            Map<?, ?> downstreamInfo = ensureTypeOrNull(Map.class, replItems.get("downstream"));
            if (downstreamInfo != null) {
                Map<?, ?> replicaVClock = ensureTypeOrNull(Map.class, downstreamInfo.get("vclock"));

                if (replicaVClock == null) {
                    return false;
                }

                Number replicaLsn = ensureTypeOrNull(Number.class, replicaVClock.get(masterId));

                if (replicaLsn == null || replicaLsn.longValue() < masterLsn.longValue()) {
                    return false;
                }
            }
        }
        return true;
    }

    public static String toLuaSelect(String spaceName, Object key) {
        StringBuilder sb = new StringBuilder("box.space.");
        sb.append(spaceName);
        sb.append(":select{");
        appendKey(sb, key);
        sb.append("}");
        return sb.toString();
    }

    public static String toLuaDelete(String spaceName, Object key) {
        StringBuilder sb = new StringBuilder("box.space.");
        sb.append(spaceName);
        sb.append(":delete{");
        appendKey(sb, key);
        sb.append("}");
        return sb.toString();
    }

    private static void appendKey(StringBuilder sb, Object key) {
        if (List.class.isAssignableFrom(key.getClass())) {
            List parts = (List) key;
            for (int i = 0; i < parts.size(); i++) {
                if (i != 0) {
                    sb.append(", ");
                }
                Object k = parts.get(i);
                if (k instanceof BigInteger) {
                    appendBigInteger(sb, (BigInteger) k);
                } else if (k instanceof String) {
                    sb.append('\'');
                    sb.append(k);
                    sb.append('\'');
                } else {
                    sb.append(k);
                }
            }
        } else if (key instanceof BigInteger) {
            appendBigInteger(sb, (BigInteger) key);
        } else {
            sb.append(key);
        }
    }

    private static void appendBigInteger(StringBuilder sb, BigInteger value) {
        sb.append(value);
        sb.append(value.signum() >= 0 ? "ULL" : "LL");
    }

    private static <T> T ensureTypeOrNull(Class<T> cls, Object v) {
        return v == null ? null : ensureType(cls, v);
    }

    private static <T> T ensureType(Class<T> cls, Object v) {
        if (v == null || !cls.isAssignableFrom(v.getClass())) {
            throw new IllegalArgumentException(String.format("Wrong value type '%s', expected '%s'.",
                v == null ? "null" : v.getClass().getName(), cls.getName()));
        }
        return cls.cast(v);
    }

    public static String extractRawHostAndPortString(SocketAddress socketAddress) {
        InetSocketAddress inetSocketAddress = (InetSocketAddress) socketAddress;
        return inetSocketAddress.getAddress().getHostName() + ":" + inetSocketAddress.getPort();
    }

    public static Iterable<String> asRawHostAndPort(Collection<SocketAddress> addresses) {
        return addresses.stream()
            .map(TestUtils::extractRawHostAndPortString)
            .collect(Collectors.toList());
    }

    public static TarantoolClusterClientConfig makeDefaultClusterClientConfig() {
        TarantoolClusterClientConfig config = new TarantoolClusterClientConfig();
        config.username = TarantoolTestHelper.USERNAME;
        config.password = TarantoolTestHelper.PASSWORD;
        config.initTimeoutMillis = 2000;
        config.operationExpiryTimeMillis = 2000;
        config.sharedBufferSize = 128;
        config.executor = null;
        return config;
    }

    public static TarantoolClientConfig makeDefaultClientConfig() {
        TarantoolClientConfig config = new TarantoolClientConfig();
        config.username = TarantoolTestHelper.USERNAME;
        config.password = TarantoolTestHelper.PASSWORD;
        config.initTimeoutMillis = 2000;
        config.sharedBufferSize = 128;
        return config;
    }

    /**
     * Wraps a socket channel provider
     * {@link SocketChannelProvider#get(int, Throwable)} method.
     * When an error is raised the wrapper substitutes
     * this error by the predefined one. The original value is
     * still accessible as a cause of the injected error.
     *
     * @param provider provider to be wrapped
     * @param error error to be thrown instead of original
     *
     * @return wrapped provider
     */
    public static SocketChannelProvider wrapByErroredProvider(SocketChannelProvider provider, RuntimeException error) {
        return new SocketChannelProvider() {
            private final SocketChannelProvider delegate = provider;

            @Override
            public SocketChannel get(int retryNumber, Throwable lastError) {
                try {
                    return delegate.get(retryNumber, lastError);
                } catch (Exception e) {
                    error.initCause(e);
                    throw error;
                }
            }
        };
    }

    /**
     * Searches recursively the given cause for a root error.
     *
     * @param error root error
     * @param cause cause to be found
     *
     * @return {@literal true} if cause is found within a cause chain
     */
    public static boolean findCause(Throwable error, Throwable cause) {
        while (error.getCause() != null) {
            error = error.getCause();
            if (cause.equals(error)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Searches recursively the first cause being the given class type.
     *
     * @param error root error
     * @param type cause class to be found
     *
     * @return {@literal true} if cause is found within a cause chain
     */
    public static boolean findCause(Throwable error, Class<?> type) {
        while (error.getCause() != null) {
            error = error.getCause();
            if (type == error.getClass()) {
                return true;
            }
        }
        return false;
    }

}
