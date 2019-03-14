package org.tarantool;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;
import org.mockito.Matchers;
import org.mockito.Mockito;
import org.tarantool.cluster.ClusterTopologyDiscoverer;
import org.tarantool.server.TarantoolInstanceInfo;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.tarantool.AbstractTarantoolConnectorIT.makeClusterClientConfig;
import static org.tarantool.TestUtils.makeInstanceEnv;

public class ClientReconnectClusterIT {
    private static final int TIMEOUT = 500;
    private static final String LUA_FILE = "jdk-testing.lua";
    private static final String SRV1 = "replica1";
    private static final String SRV2 = "replica2";
    private static final String SRV3 = "replica3";
    private static final int[] PORTS = {3302, 3303, 3304};
    private static final int[] CONSOLE_PORTS = {3312, 3313, 3314};
    private static TarantoolControl control;

    private static String REPLICATION_CONFIG = TestUtils.makeReplicationString(
        AbstractTarantoolConnectorIT.username,
        AbstractTarantoolConnectorIT.password,
        "localhost:" + PORTS[0],
        "localhost:" + PORTS[1],
        "localhost:" + PORTS[2]);

    // Resume replication faster in case of temporary failure to fit TIMEOUT.
    private static double REPLICATION_TIMEOUT = 0.1;

    @BeforeAll
    public static void setupEnv() {
        control = new TarantoolControl();
        int idx = 0;
        for (String name: Arrays.asList(SRV1, SRV2, SRV3)) {
            control.createInstance(name, LUA_FILE,
                makeInstanceEnv(PORTS[idx], CONSOLE_PORTS[idx], REPLICATION_CONFIG,
                                REPLICATION_TIMEOUT));
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

    @Test
    public void testRoundRobinReconnect() {
        control.start(SRV1);
        control.start(SRV2);
        control.start(SRV3);

        control.waitStarted(SRV1);
        control.waitStarted(SRV2);
        control.waitStarted(SRV3);

        final TarantoolClientImpl client = makeClient(
            "localhost:" + PORTS[0],
            "127.0.0.1:" + PORTS[1],
            "localhost:" + PORTS[2]);

        List<?> ids = client.syncOps().eval(
            "return box.schema.space.create('rr_test').id, " +
            "box.space.rr_test:create_index('primary').id");

        final int spaceId = ((Number)ids.get(0)).intValue();
        final int pkId = ((Number)ids.get(1)).intValue();

        final List<?> key = Collections.singletonList(1);
        final List<?> tuple = Arrays.asList(1, 1);

        client.syncOps().insert(spaceId, tuple);
        control.waitReplication(SRV1, TIMEOUT);

        List<?> res = client.syncOps().select(spaceId, pkId, key, 0, 1, Iterator.EQ);
        assertEquals(res.get(0), tuple);

        control.stop(SRV1);

        res = client.syncOps().select(spaceId, pkId, key, 0, 1, Iterator.EQ);
        assertEquals(res.get(0), Arrays.asList(1, 1));

        control.stop(SRV2);

        res = client.syncOps().select(spaceId, pkId, key, 0, 1, Iterator.EQ);
        assertEquals(res.get(0), Arrays.asList(1, 1));

        control.stop(SRV3);

        CommunicationException e = assertThrows(CommunicationException.class, new Executable() {
            @Override
            public void execute() throws Throwable {
                client.syncOps().select(spaceId, pkId, key, 0, 1, Iterator.EQ);
            }
        });

        assertEquals("Connection time out.", e.getMessage());
    }

    private TarantoolClusterClient makeClient(String...addrs) {
        TarantoolClusterClientConfig config = makeClusterClientConfig();
        config.slaveHosts = addrs;
        return new TarantoolClusterClient(config);
    }


    @Test
    @Disabled("Incomplete implementation")
    void testUpdateNodeList() {
        control.start(SRV1);
        control.start(SRV2);
        control.start(SRV3);

        control.waitStarted(SRV1);
        control.waitStarted(SRV2);
        control.waitStarted(SRV3);


        String testSchemaCreateScript = "return box.schema.space.create('rr_test').id, " +
                "box.space.rr_test:create_index('primary').id";
        control.executeCommand(testSchemaCreateScript, SRV1);

        String srv1_address = "localhost:" + PORTS[0];
        String srv2_address = "127.0.0.1:" + PORTS[1];
        String srv3_address = "localhost:" + PORTS[2];

        String INFO_FUNCTION_NAME = "returnAddrsExceptSrv1";
        String INFO_FUNCTION_SCRIPT =
                "function " + INFO_FUNCTION_NAME + "() return {'" + srv2_address + "', '" + srv3_address + "'} end";

        control.executeCommand(INFO_FUNCTION_SCRIPT, SRV1);
        control.waitReplication(SRV1, TIMEOUT);

        final TarantoolClusterClient client = makeClient(
                srv1_address,
                srv2_address);
        List<?> ids = client.syncOps().eval(
                testSchemaCreateScript);


//todo
        List<TarantoolInstanceInfo> newInstances = Stream.of(srv2_address, srv3_address)
                .map(TarantoolInstanceInfo::create)
                .collect(Collectors.toList());

        ClusterTopologyDiscoverer discovererMock = Mockito.mock(ClusterTopologyDiscoverer.class);
        Mockito.when(discovererMock.discoverTarantoolInstances(Matchers.anyInt())).thenReturn(newInstances);


    }
}
