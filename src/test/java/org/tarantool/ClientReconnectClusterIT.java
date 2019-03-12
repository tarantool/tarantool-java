package org.tarantool;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.tarantool.AbstractTarantoolConnectorIT.makeClusterClientConfig;
import static org.tarantool.TestUtils.makeDiscoveryFunction;
import static org.tarantool.TestUtils.makeInstanceEnv;

import org.tarantool.cluster.ClusterServiceStoredFunctionDiscovererIT;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@DisplayName("A cluster client")
public class ClientReconnectClusterIT {

    private static final String SCHEMA_PATTERN =
        "return box.schema.space.create('%1$s').id, box.space.%1$s:create_index('primary').id";

    private static final int TIMEOUT = 500;
    private static final String LUA_FILE = "jdk-testing.lua";
    private static final String SRV1 = "replica1";
    private static final String SRV2 = "replica2";
    private static final String SRV3 = "replica3";
    private static final int[] PORTS = { 3302, 3303, 3304 };
    private static final int[] CONSOLE_PORTS = { 3312, 3313, 3314 };
    private static TarantoolControl control;

    private static String REPLICATION_CONFIG = TestUtils.makeReplicationString(
        AbstractTarantoolConnectorIT.username,
        AbstractTarantoolConnectorIT.password,
        "localhost:" + PORTS[0],
        "localhost:" + PORTS[1],
        "localhost:" + PORTS[2]
    );

    // Resume replication faster in case of temporary failure to fit TIMEOUT.
    private static double REPLICATION_TIMEOUT = 0.1;

    @BeforeAll
    public static void setupEnv() {
        control = new TarantoolControl();
        int idx = 0;
        for (String name : Arrays.asList(SRV1, SRV2, SRV3)) {
            control.createInstance(name, LUA_FILE,
                makeInstanceEnv(
                    PORTS[idx], CONSOLE_PORTS[idx],
                    REPLICATION_CONFIG, REPLICATION_TIMEOUT
                )
            );
            idx++;
        }
    }

    @AfterAll
    public static void tearDownEnv() {
        for (String name : Arrays.asList(SRV1, SRV2, SRV3)) {
            control.stop(name);
            /*
             * Don't cleanup instance directory to allow further investigation
             * of xlog / snap files in case of the test failure.
             */
        }
    }

    @BeforeEach
    public void setUpTest() {
        startInstancesAndAwait(SRV1, SRV2, SRV3);
    }

    @AfterEach
    public void tearDownTest() {
        stopInstancesAndAwait(SRV1, SRV2, SRV3);
    }

    @Test
    @DisplayName("reconnected to another node after the current node had disappeared")
    public void testRoundRobinReconnect() {
        final TarantoolClientImpl client = makeClusterClient(
            "localhost:" + PORTS[0],
            "127.0.0.1:" + PORTS[1],
            "localhost:" + PORTS[2]
        );

        int[] ids = makeAndFillTestSpace(client, "rr_test1");
        final int spaceId = ids[0];
        final int pkId = ids[1];

        expectConnected(client, spaceId, pkId);

        stopInstancesAndAwait(SRV1);
        expectConnected(client, spaceId, pkId);

        stopInstancesAndAwait(SRV2);
        expectConnected(client, spaceId, pkId);

        stopInstancesAndAwait(SRV3);
        expectDisconnected(client, spaceId, pkId);
    }

    /**
     * Before fetch <code>client = { srv1 }</code>
     * After fetch <code>client = { srv1, srv2 }</code>
     * <p>
     * 1. fetch nodes - ok (client will apply { srv1, srv2 } as a new nodes list)
     * 2. shutdown srv1 - ok (client will reconnect to srv2)
     * 3. shutdown srv2 - fail (there are no available nodes anymore)
     */
    @Test
    @DisplayName("applied new nodes and reconnected to another node")
    void testUpdateExtendedNodeList() {
        String service1Address = "localhost:" + PORTS[0];
        String service2Address = "127.0.0.1:" + PORTS[1];
        String service3Address = "localhost:" + PORTS[2];

        CyclicBarrier barrier = new CyclicBarrier(2);

        String infoFunctionName = "getAddresses";
        String infoFunctionScript =
            makeDiscoveryFunction(infoFunctionName, Arrays.asList(service1Address, service2Address));

        control.openConsole(SRV1).exec(infoFunctionScript);

        final TarantoolClusterClient client = makeClientWithDiscoveryFeature(
            infoFunctionName,
            0,
            (ignored) -> tryAwait(barrier),
            service1Address
        );

        int[] ids = makeAndFillTestSpace(client, "rr_test2");
        final int spaceId = ids[0];
        final int pkId = ids[1];

        tryAwait(barrier); // client = { srv1 }; wait for { srv1, srv2 }

        expectConnected(client, spaceId, pkId);

        stopInstancesAndAwait(SRV1);
        expectConnected(client, spaceId, pkId);

        stopInstancesAndAwait(SRV2);
        expectDisconnected(client, spaceId, pkId);

        stopInstancesAndAwait(SRV3);
    }

    /**
     * Before fetch <code>client = { srv1, srv2 }</code>
     * After fetch <code>client = { srv1 }</code>
     * <p>
     * 1. fetch nodes - ok (client will apply the narrowed { srv1 }
     * 2. shutdown srv1 - fail (client will not reconnect to srv2 because latest is out of the list)
     */
    @Test
    @DisplayName("applied new nodes and stayed connected to the current node")
    void testUpdateNarrowNodeList() {
        String service1Address = "localhost:" + PORTS[0];
        String service2Address = "127.0.0.1:" + PORTS[1];

        CyclicBarrier barrier = new CyclicBarrier(2);

        String infoFunctionName = "getAddresses";
        String infoFunctionScript = makeDiscoveryFunction(infoFunctionName, Collections.singletonList(service1Address));

        control.openConsole(SRV1).exec(infoFunctionScript);

        final TarantoolClusterClient client = makeClientWithDiscoveryFeature(
            infoFunctionName,
            0,
            (ignored) -> tryAwait(barrier),
            service1Address,
            service2Address
        );

        int[] ids = makeAndFillTestSpace(client, "rr_test3");
        final int spaceId = ids[0];
        final int pkId = ids[1];

        tryAwait(barrier); // client = { srv1, srv2 }; wait for { srv1 }

        expectConnected(client, spaceId, pkId);

        stopInstancesAndAwait(SRV1);
        expectDisconnected(client, spaceId, pkId);

        stopInstancesAndAwait(SRV2);
        stopInstancesAndAwait(SRV3);
    }

    /**
     * Before fetch <code>client = { srv1, srv2, srv3 }</code>
     * After fetch <code>client = { srv1, srv2, srv3 }</code>
     * <p>
     * 1. fetch nodes - ok (client will ignore the same list)
     * 2. shutdown srv1 - ok
     * 3. shutdown srv2 - ok
     * 4. shutdown srv3 - fail
     */
    @Test
    @DisplayName("applied empty list and stayed connected to the current node")
    void testUpdateEmptyNodeList() {
        String service1Address = "localhost:" + PORTS[0];
        String service2Address = "127.0.0.1:" + PORTS[1];
        String service3Address = "localhost:" + PORTS[2];

        String infoFunctionName = "getAddresses";
        String infoFunctionScript = makeDiscoveryFunction(infoFunctionName, Collections.emptyList());

        control.openConsole(SRV1).exec(infoFunctionScript);

        final TarantoolClusterClient client = makeClientWithDiscoveryFeature(
            infoFunctionName,
            service1Address,
            service2Address,
            service3Address
        );

        int[] ids = makeAndFillTestSpace(client, "rr_test4");
        final int spaceId = ids[0];
        final int pkId = ids[1];

        expectConnected(client, spaceId, pkId);

        stopInstancesAndAwait(SRV1);
        expectConnected(client, spaceId, pkId);

        stopInstancesAndAwait(SRV2);
        expectConnected(client, spaceId, pkId);

        stopInstancesAndAwait(SRV3);
        expectDisconnected(client, spaceId, pkId);
    }

    /**
     * Before fetch <code>client = { srv1, srv2, srv3 }</code>
     * After fetch <code>client = { srv1, srv2, srv3 }</code>
     * <p>
     * 1. fetch with an exception (i.e. missing/error-prone function) - ok (client will ignore the failure)
     * 2. shutdown srv1 - ok
     * 3. shutdown srv2 - ok
     * 4. shutdown srv3 - fail
     *
     * @see ClusterServiceStoredFunctionDiscovererIT#testFunctionWithError()
     */
    @Test
    @DisplayName("applied nothing and stayed connected to the current node")
    void testWrongConfigFetch() {
        String service1Address = "localhost:" + PORTS[0];
        String service2Address = "127.0.0.1:" + PORTS[1];
        String service3Address = "localhost:" + PORTS[2];

        String infoFunctionName = "getAddresses";
        String infoFunctionScript = makeDiscoveryFunction(infoFunctionName, Collections.emptyList());

        control.openConsole(SRV1).exec(infoFunctionScript);

        final TarantoolClusterClient client = makeClientWithDiscoveryFeature(
            infoFunctionName + "wrongSuffix",
            service1Address,
            service2Address,
            service3Address
        );

        int[] ids = makeAndFillTestSpace(client, "rr_test5");
        final int spaceId = ids[0];
        final int pkId = ids[1];

        expectConnected(client, spaceId, pkId);

        stopInstancesAndAwait(SRV1);
        expectConnected(client, spaceId, pkId);

        stopInstancesAndAwait(SRV2);
        expectConnected(client, spaceId, pkId);

        stopInstancesAndAwait(SRV3);
        expectDisconnected(client, spaceId, pkId);
    }

    /**
     * Before fetch <code>client = { srv1, srv2, srv3 }</code>
     * After fetch <code>client = { srv1, srv2, srv3 }</code>
     * <p>
     * 1. fetch an improper result - ok (client will ignore the awkward data)
     * 2. shutdown srv1 - ok
     * 3. shutdown srv2 - ok
     * 4. shutdown srv3 - fail
     */
    @Test
    @DisplayName("ignored an wrong function result and stayed connected to the current node")
    void testWrongFunctionResultFetch() {
        String service1Address = "localhost:" + PORTS[0];
        String service2Address = "127.0.0.1:" + PORTS[1];
        String service3Address = "localhost:" + PORTS[2];

        String infoFunctionName = "getWhateverExceptAddressesListFunction";
        String infoFunctionScript = makeDiscoveryFunction(infoFunctionName, 42);

        control.openConsole(SRV1).exec(infoFunctionScript);

        final TarantoolClusterClient client = makeClientWithDiscoveryFeature(
            infoFunctionName,
            service1Address,
            service2Address,
            service3Address
        );

        int[] ids = makeAndFillTestSpace(client, "rr_test6");
        final int spaceId = ids[0];
        final int pkId = ids[1];

        expectConnected(client, spaceId, pkId);

        stopInstancesAndAwait(SRV1);
        expectConnected(client, spaceId, pkId);

        stopInstancesAndAwait(SRV2);
        expectConnected(client, spaceId, pkId);

        stopInstancesAndAwait(SRV3);
        expectDisconnected(client, spaceId, pkId);
    }

    /**
     * Before fetch <code>client = { srv1 }</code>
     * After fetch ph1 <code>client = { srv1 }</code>
     * After fetch ph2 <code>client = { srv2 }</code>
     * After fetch ph3 <code>client = { srv3 }</code>
     * <p>
     * 1. fetch an initial result (ph1) - ok (client will ignore the same data)
     * 2. fetch the 2nd result (ph2) - ok (client will reconnect to srv2)
     * 3. shutdown srv1 - ok
     * 4. fetch the 3rd result (ph3) - ok (client will reconnect to srv3)
     * 5. shutdown srv2 - ok
     * 6. shutdown srv3 - fail
     */
    @Test
    @DisplayName("applied each second a new cluster node and reconnected to it")
    void testDelayFunctionResultFetch() {
        String service1Address = "localhost:" + PORTS[0];
        String service2Address = "127.0.0.1:" + PORTS[1];
        String service3Address = "localhost:" + PORTS[2];

        CyclicBarrier barrier = new CyclicBarrier(2);

        String infoFunctionName = "getAddressesFunction";
        String functionBody = Stream.of(service1Address, service2Address)
            .map(address -> "coroutine.yield('" + address + "');")
            .collect(Collectors.joining(" "));

        control.openConsole(SRV1)
            .exec("co = coroutine.create(function() " + functionBody + " end)");
        control.openConsole(SRV1)
            .exec("function getAddressesFunction() local c, r = coroutine.resume(co); return r end");

        String infoFunctionScript = makeDiscoveryFunction(infoFunctionName, Collections.singletonList(service3Address));
        control.openConsole(SRV2).exec(infoFunctionScript);

        final TarantoolClusterClient client = makeClientWithDiscoveryFeature(
            infoFunctionName,
            3000,
            (ignored) -> tryAwait(barrier),
            service1Address
        );

        int[] ids = makeAndFillTestSpace(client, "rr_test7");
        final int spaceId = ids[0];
        final int pkId = ids[1];

        tryAwait(barrier); // client = { srv1 }; wait for { srv1 }

        expectConnected(client, spaceId, pkId);

        tryAwait(barrier); // client = { srv1 }; wait for { srv2 }

        stopInstancesAndAwait(SRV1);
        expectConnected(client, spaceId, pkId);

        tryAwait(barrier); // client = { srv2 }; wait for { srv3 }

        stopInstancesAndAwait(SRV2);
        expectConnected(client, spaceId, pkId);

        stopInstancesAndAwait(SRV3);
        expectDisconnected(client, spaceId, pkId);
    }

    private void tryAwait(CyclicBarrier barrier) {
        try {
            barrier.await(6000, TimeUnit.MILLISECONDS);
        } catch (Throwable e) {
            e.printStackTrace();
        }
    }

    private void startInstancesAndAwait(String... instances) {
        for (String instance : instances) {
            control.start(instance);
        }
        for (String instance : instances) {
            control.waitStarted(instance);
        }
    }

    private void stopInstancesAndAwait(String... instances) {
        for (String instance : instances) {
            control.stop(instance);
        }
        for (String instance : instances) {
            control.waitStopped(instance);
        }
    }

    private void expectConnected(TarantoolClientImpl client, int spaceId, int pkId) {
        final List<?> key = Collections.singletonList(1);
        final List<?> tuple = Arrays.asList(1, 1);

        List<?> res = client.syncOps().select(spaceId, pkId, key, 0, 1, Iterator.EQ);
        assertEquals(res.get(0), tuple);
    }

    private void expectDisconnected(TarantoolClientImpl client, int spaceId, int pkId) {
        final List<?> key = Collections.singletonList(1);

        assertThrows(
            CommunicationException.class,
            () -> client.syncOps().select(spaceId, pkId, key, 0, 1, Iterator.EQ)
        );
    }

    private int[] makeAndFillTestSpace(TarantoolClientImpl client, String spaceName) {
        List<?> ids = client.syncOps().eval(String.format(SCHEMA_PATTERN, spaceName));

        final int spaceId = ((Number) ids.get(0)).intValue();
        final int pkId = ((Number) ids.get(1)).intValue();

        client.syncOps().insert(spaceId, Arrays.asList(1, 1));
        control.waitReplication(SRV1, TIMEOUT);

        return new int[] { spaceId, pkId };
    }

    private TarantoolClusterClient makeClusterClient(String... addresses) {
        return makeClientWithDiscoveryFeature(null, addresses);
    }

    private TarantoolClusterClient makeClientWithDiscoveryFeature(String entryFunction,
                                                                  String... addresses) {
        return makeClientWithDiscoveryFeature(entryFunction, 0, null, addresses);
    }

    private TarantoolClusterClient makeClientWithDiscoveryFeature(String entryFunction,
                                                                  int entryDelayMillis,
                                                                  Consumer<Set<String>> consumer,
                                                                  String... addresses) {
        TarantoolClusterClientConfig config = makeClusterClientConfig();
        config.clusterDiscoveryEntryFunction = entryFunction;
        config.clusterDiscoveryDelayMillis = entryDelayMillis;

        return new TarantoolClusterClient(config, addresses) {
            @Override
            protected void onInstancesRefreshed(Set<String> instances) {
                super.onInstancesRefreshed(instances);
                if (consumer != null) {
                    consumer.accept(instances);
                }
            }
        };
    }

}
