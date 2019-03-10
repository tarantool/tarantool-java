package org.tarantool.cluster;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.tarantool.AbstractTarantoolConnectorIT;
import org.tarantool.TarantoolClusterClientConfig;
import org.tarantool.TarantoolControl;
import org.tarantool.server.TarantoolInstanceInfo;

import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;
import static org.tarantool.TestUtils.makeInstanceEnv;

@DisplayName("ClusterTopologyFromFunctionDiscovererImpl integration tests")
class ClusterTopologyFromFunctionDiscovererImplIT {
    protected static final int INSTANCE_LISTEN_PORT = 3301;
    protected static final int INSTANCE_ADMIN_PORT = 3313;
    private static final String LUA_FILE = "jdk-testing.lua";

    private static final String INSTANCE_NAME = "jdk-testing";
    private static TarantoolControl control;
    private static TarantoolClusterClientConfig clusterConfig;

    private static String INFO_FUNCTION_NAME = "retAddrs";
    private static String TEST_RETURN_HOSTLIST_SCRIPT =
            "function " + INFO_FUNCTION_NAME + "() return {'localhost:80', '127.0.0.1:3301'} end";

    private static final Integer DISCOVER_TIMEOUT_MILLIS = 1000;

    @BeforeAll
    public static void setupEnv() {
        control = new TarantoolControl();
        control.createInstance(INSTANCE_NAME, LUA_FILE, makeInstanceEnv(INSTANCE_LISTEN_PORT, INSTANCE_ADMIN_PORT));

        control.start(INSTANCE_NAME);
        control.waitStarted("jdk-testing");

        clusterConfig = AbstractTarantoolConnectorIT.makeClusterClientConfig();
        clusterConfig.infoHost = "localhost:" + INSTANCE_LISTEN_PORT;
        clusterConfig.infoFunctionName = INFO_FUNCTION_NAME;
    }

    @AfterAll
    public static void tearDownEnv() {
        control.stop(INSTANCE_NAME);
    }

    @Test
    @DisplayName("Discoverer successfully fetches and parses a node list from an info node.")
    void testSuccessfulAddressParsing() {
        //inject the function
        control.openConsole(INSTANCE_NAME).exec(TEST_RETURN_HOSTLIST_SCRIPT);
        ClusterTopologyFromFunctionDiscovererImpl discoverer =
                new ClusterTopologyFromFunctionDiscovererImpl(clusterConfig);


        List<TarantoolInstanceInfo> instances = discoverer.discoverTarantoolInstances(DISCOVER_TIMEOUT_MILLIS);


        assertNotNull(instances);
        assertEquals(2, instances.size());
        assertTrue(instances.contains(TarantoolInstanceInfo.create("localhost:80")));
        assertTrue(instances.contains(TarantoolInstanceInfo.create("127.0.0.1:3301")));
    }

    @Test
    @DisplayName("Gracefully process case when the function returns empty node list")
    void testFunctionReturnedEmptyList() {
        String functionCode = "function " + INFO_FUNCTION_NAME + "() return {} end";
        //inject the function
        control.openConsole(INSTANCE_NAME).exec(functionCode);
        ClusterTopologyFromFunctionDiscovererImpl discoverer =
                new ClusterTopologyFromFunctionDiscovererImpl(clusterConfig);


        List<TarantoolInstanceInfo> instances = discoverer.discoverTarantoolInstances(DISCOVER_TIMEOUT_MILLIS);


        assertNotNull(instances);
        assertTrue(instances.isEmpty());
    }
}