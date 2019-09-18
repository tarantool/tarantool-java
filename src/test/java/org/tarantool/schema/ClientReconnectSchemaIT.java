package org.tarantool.schema;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.tarantool.TestUtils.makeDefaultClusterClientConfig;

import org.tarantool.Iterator;
import org.tarantool.TarantoolClientImpl;
import org.tarantool.TarantoolClusterClient;
import org.tarantool.TarantoolClusterClientConfig;
import org.tarantool.TarantoolTestHelper;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class ClientReconnectSchemaIT {

    private static final String[] SRVS = { "srv-schema-it-1", "srv-schema-it-2" };
    private static final int[] PORTS = { 3401, 3402 };

    private static TarantoolTestHelper firstTestHelper;
    private static TarantoolTestHelper secondTestHelper;

    @BeforeAll
    public static void setupEnv() {
        firstTestHelper = new TarantoolTestHelper(SRVS[0]);
        firstTestHelper.createInstance(TarantoolTestHelper.LUA_FILE, PORTS[0], PORTS[0] + 1000);
        firstTestHelper.startInstance();

        secondTestHelper = new TarantoolTestHelper(SRVS[1]);
        secondTestHelper.createInstance(TarantoolTestHelper.LUA_FILE, PORTS[1], PORTS[1] + 1000);
        secondTestHelper.startInstance();
    }

    @AfterAll
    public static void teardownEnv() {
        firstTestHelper.stopInstance();
        secondTestHelper.stopInstance();
    }

    @Test
    @DisplayName("got a result from another node after the current node had disappeared")
    public void testSameNamedSpaceAfterReconnection() {
        String[] firstSpace = {
            "box.schema.space.create('string_space1', { format = { {name = 'id', type = 'integer'} } })",
            "box.space.string_space1:create_index('primary', { type = 'TREE', parts = {'id'} })"
        };
        String[] secondSpace = {
            "box.schema.space.create('string_space2', { format = { {name = 'id', type = 'integer'} } })",
            "box.space.string_space2:create_index('primary', { type = 'TREE', parts = {'id'} })"
        };

        // create spaces on two instances with an inverted order
        // as a result, instances have same schema version but spaces have unequal IDs
        firstTestHelper.executeLua(firstSpace);
        firstTestHelper.executeLua(secondSpace);
        firstTestHelper.executeLua("box.space.string_space1:insert{100}");
        secondTestHelper.executeLua(secondSpace);
        secondTestHelper.executeLua(firstSpace);
        secondTestHelper.executeLua("box.space.string_space1:insert{200}");
        assertEquals(firstTestHelper.getInstanceVersion(), secondTestHelper.getInstanceVersion());

        int firstSpaceIdFirstInstance = firstTestHelper.evaluate("box.space.string_space1.id");
        int firstSpaceIdSecondInstance = secondTestHelper.evaluate("box.space.string_space1.id");
        assertNotEquals(firstSpaceIdFirstInstance, firstSpaceIdSecondInstance);

        final TarantoolClientImpl client = makeClusterClient(
            "localhost:" + PORTS[0],
            "127.0.0.1:" + PORTS[1]
        );

        List<?> result = client.syncOps()
            .select("string_space1", "primary", Collections.emptyList(), 0, 10, Iterator.ALL);
        assertEquals(Arrays.asList(100), result.get(0));
        firstTestHelper.stopInstance();

        result = client.syncOps()
            .select("string_space1", "primary", Collections.emptyList(), 0, 10, Iterator.ALL);
        assertEquals(Arrays.asList(200), result.get(0));
        secondTestHelper.stopInstance();
    }

    private TarantoolClusterClient makeClusterClient(String... addresses) {
        TarantoolClusterClientConfig config = makeDefaultClusterClientConfig();
        return new TarantoolClusterClient(config, addresses);
    }

}
