package org.tarantool.cluster;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.tarantool.TestUtils.makeDiscoveryFunction;
import static org.tarantool.TestUtils.makeInstanceEnv;

import org.tarantool.AbstractTarantoolConnectorIT;
import org.tarantool.CommunicationException;
import org.tarantool.TarantoolClient;
import org.tarantool.TarantoolClientImpl;
import org.tarantool.TarantoolClusterClientConfig;
import org.tarantool.TarantoolControl;
import org.tarantool.TarantoolException;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;

@DisplayName("A cluster discoverer")
public class ClusterServiceStoredFunctionDiscovererIT {

    protected static final int INSTANCE_LISTEN_PORT = 3301;
    protected static final int INSTANCE_ADMIN_PORT = 3313;
    private static final String LUA_FILE = "jdk-testing.lua";

    private static final String INSTANCE_NAME = "jdk-testing";
    private static TarantoolControl control;

    private static String ENTRY_FUNCTION_NAME = "getAddresses";

    private TarantoolClusterClientConfig clusterConfig;
    private TarantoolClient client;

    @BeforeAll
    public static void setupEnv() {
        control = new TarantoolControl();
        control.createInstance(INSTANCE_NAME, LUA_FILE, makeInstanceEnv(INSTANCE_LISTEN_PORT, INSTANCE_ADMIN_PORT));

        control.start(INSTANCE_NAME);
    }

    @BeforeEach
    public void setupTest() {
        clusterConfig = AbstractTarantoolConnectorIT.makeClusterClientConfig();
        clusterConfig.clusterDiscoveryEntryFunction = ENTRY_FUNCTION_NAME;

        client = new TarantoolClientImpl("localhost:" + INSTANCE_LISTEN_PORT, clusterConfig);
    }

    @AfterAll
    public static void tearDownEnv() {
        control.stop(INSTANCE_NAME);
    }

    @Test
    @DisplayName("fetched list of addresses")
    public void testSuccessfulAddressParsing() {
        List<String> addresses = Arrays.asList("localhost:3311", "127.0.0.1:3301");
        String functionCode = makeDiscoveryFunction(ENTRY_FUNCTION_NAME, addresses);
        control.openConsole(INSTANCE_NAME).exec(functionCode);

        TarantoolClusterStoredFunctionDiscoverer discoverer =
                new TarantoolClusterStoredFunctionDiscoverer(clusterConfig, client);

        Set<String> instances = discoverer.getInstances();

        assertNotNull(instances);
        assertEquals(2, instances.size());
        assertTrue(instances.contains(addresses.get(0)));
        assertTrue(instances.contains(addresses.get(1)));
    }

    @Test
    @DisplayName("fetched duplicated addresses")
    public void testSuccessfulUniqueAddressParsing() {
        List<String> addresses = Arrays.asList("localhost:3311", "127.0.0.1:3301", "127.0.0.2:3301", "localhost:3311");

        String functionCode = makeDiscoveryFunction(ENTRY_FUNCTION_NAME, addresses);
        control.openConsole(INSTANCE_NAME).exec(functionCode);

        TarantoolClusterStoredFunctionDiscoverer discoverer =
                new TarantoolClusterStoredFunctionDiscoverer(clusterConfig, client);

        Set<String> instances = discoverer.getInstances();

        assertNotNull(instances);
        assertEquals(3, instances.size());
        assertTrue(instances.contains(addresses.get(0)));
        assertTrue(instances.contains(addresses.get(1)));
        assertTrue(instances.contains(addresses.get(3)));
    }


    @Test
    @DisplayName("fetched empty address list")
    public void testFunctionReturnedEmptyList() {
        String functionCode = makeDiscoveryFunction(ENTRY_FUNCTION_NAME, Collections.emptyList());
        control.openConsole(INSTANCE_NAME).exec(functionCode);

        TarantoolClusterStoredFunctionDiscoverer discoverer =
                new TarantoolClusterStoredFunctionDiscoverer(clusterConfig, client);

        Set<String> instances = discoverer.getInstances();

        assertNotNull(instances);
        assertTrue(instances.isEmpty());
    }

    @Test
    @DisplayName("fetched with an exception using wrong function name")
    public void testWrongFunctionName() {
        clusterConfig.clusterDiscoveryEntryFunction = "wrongFunction";

        TarantoolClusterStoredFunctionDiscoverer discoverer =
                new TarantoolClusterStoredFunctionDiscoverer(clusterConfig, client);

        assertThrows(TarantoolException.class, discoverer::getInstances);
    }

    @Test
    @DisplayName("fetched with an exception using a broken client")
    public void testWrongInstanceAddress() {
        clusterConfig.initTimeoutMillis = 1000;

        client.close();
        TarantoolClusterStoredFunctionDiscoverer discoverer =
                new TarantoolClusterStoredFunctionDiscoverer(clusterConfig, client);

        assertThrows(CommunicationException.class, discoverer::getInstances);
    }

    @Test
    @DisplayName("fetched with an exception when wrong data type returned")
    public void testWrongTypeResultData() {
        String functionCode = makeDiscoveryFunction(ENTRY_FUNCTION_NAME, 42);
        control.openConsole(INSTANCE_NAME).exec(functionCode);

        TarantoolClusterStoredFunctionDiscoverer discoverer =
                new TarantoolClusterStoredFunctionDiscoverer(clusterConfig, client);

        assertThrows(IllegalDiscoveryFunctionResult.class, discoverer::getInstances);
    }

    @Test
    @DisplayName("fetched with an exception using no return function")
    public void testFunctionWithNoReturn() {
        String functionCode = makeDiscoveryFunction(ENTRY_FUNCTION_NAME, "");
        control.openConsole(INSTANCE_NAME).exec(functionCode);

        TarantoolClusterStoredFunctionDiscoverer discoverer =
            new TarantoolClusterStoredFunctionDiscoverer(clusterConfig, client);

        assertThrows(IllegalDiscoveryFunctionResult.class, discoverer::getInstances);
    }

    @Test
    @DisplayName("fetched first result as a list and ignored other multi-results")
    public void testWrongMultiResultData() {
        String functionCode = makeDiscoveryFunction(ENTRY_FUNCTION_NAME, "{'host1'}, 'host2', 423");
        control.openConsole(INSTANCE_NAME).exec(functionCode);

        TarantoolClusterStoredFunctionDiscoverer discoverer =
                new TarantoolClusterStoredFunctionDiscoverer(clusterConfig, client);

        Set<String> instances = discoverer.getInstances();

        assertNotNull(instances);
        assertEquals(1, instances.size());
        assertTrue(instances.contains("host1"));
    }

    @Test
    @DisplayName("fetched with an exception using error-prone function")
    public void testFunctionWithError() {
        String functionCode = makeDiscoveryFunction(ENTRY_FUNCTION_NAME, "error('msg')");
        control.openConsole(INSTANCE_NAME).exec(functionCode);

        TarantoolClusterStoredFunctionDiscoverer discoverer =
                new TarantoolClusterStoredFunctionDiscoverer(clusterConfig, client);

        assertThrows(TarantoolException.class, discoverer::getInstances);
    }

}
