package org.tarantool;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TestUtils {

    static final String replicationInfoRequest = "return " +
        "box.info.id, " +
        "box.info.lsn, " +
        "box.info.replication";

    @FunctionalInterface
    public interface ThrowingAction<X extends Throwable> {
        void run() throws X;
    }

    public static <X extends Throwable> Runnable throwingWrapper(ThrowingAction<X> action) {
        return () -> {
            try {
                action.run();
            } catch (Throwable e) {
                throw new RuntimeException(e);
            }
        };
    }

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

    public static Map<String, String> makeInstanceEnv(int port, int consolePort, String replicationConfig,
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
}
