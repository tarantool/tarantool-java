package org.tarantool;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TestUtils {
    final static String replicationInfoRequest = "return " +
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

    public static Map<String, String> makeInstanceEnv(int port, int consolePort, String replicationConfig,
                                                      double replicationTimeout) {
        Map<String, String> env = makeInstanceEnv(port, consolePort);
        env.put("MASTER", replicationConfig);
        env.put("REPLICATION_TIMEOUT", Double.toString(replicationTimeout));
        return env;
    }

    /**
     * Wait until all replicas will be in sync with master's log.
     *
     * It is useful to wait until the last data modification performed on
     * **that** instance will be applied on its replicas. It does not take care
     * to modifications performed on instances, which are master's of that one.
     */
    public static void waitReplication(TarantoolClientImpl client, int timeout) {
        long deadline = System.currentTimeMillis() + timeout;
        for (;;) {
            List<?> v;
            try {
                v = client.syncOps().eval(replicationInfoRequest);
            } catch (TarantoolException ignored) {
                continue;
            }

            if (parseAndCheckReplicationStatus(v))
                return;

            if (deadline < System.currentTimeMillis())
                throw new RuntimeException("Test failure: timeout waiting for replication.");

            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }

    /**
     * See waitReplication(TarantoolClientImpl client, int timeout).
     */
    protected static void waitReplication(TarantoolConsole console, int timeout) {
        long deadline = System.currentTimeMillis() + timeout;
        for (;;) {
            List<?> v = console.evalList(replicationInfoRequest);

            if (parseAndCheckReplicationStatus(v))
                return;

            if (deadline < System.currentTimeMillis())
                throw new RuntimeException("Test failure: timeout waiting for replication.");

            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }

    private static boolean parseAndCheckReplicationStatus(List data) {
        if (data == null || data.size() != 3)
            throw new IllegalStateException("Unexpected format of replication status.");

        Number masterId = ensureType(Number.class, data.get(0));
        Number masterLsn = ensureType(Number.class, data.get(1));
        Map<?,?> replInfo = ensureTypeOrNull(Map.class, data.get(2));

        if (replInfo == null || replInfo.size() < 2)
            return false;

        for (Object info : replInfo.values()) {
            Map<?, ?> replItems = ensureTypeOrNull(Map.class, info);

            Map<?,?> downstreamInfo = ensureTypeOrNull(Map.class, replItems.get("downstream"));
            if (downstreamInfo != null) {
                Map<?, ?> replica_vclock = ensureTypeOrNull(Map.class, downstreamInfo.get("vclock"));

                if (replica_vclock == null)
                    return false;

                Number replicaLsn = ensureTypeOrNull(Number.class, replica_vclock.get(masterId));

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
