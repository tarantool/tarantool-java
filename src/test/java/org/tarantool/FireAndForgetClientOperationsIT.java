package org.tarantool;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Test "fire & forget" operations available in {@link TarantoolClientImpl} class.
 */
public class FireAndForgetClientOperationsIT extends AbstractTarantoolConnectorIT {
    private TarantoolClient client;

    @BeforeEach
    public void setup() {
        client = makeClient();
    }

    @AfterEach
    public void tearDown() {
        client.close();
    }

    @Test
    public void testPing() {
        // Half-ping actually.
        client.fireAndForgetOps().ping();
    }

    @Test
    public void testClose() {
        IllegalStateException e = assertThrows(IllegalStateException.class, new Executable() {
            @Override
            public void execute() throws Throwable {
                client.fireAndForgetOps().close();
            }
        });
        assertEquals(e.getMessage(), "You should close TarantoolClient instead.");
    }

    @Test
    public void testFireAndForgetOperations() {
        TarantoolClientOps<Integer, List<?>, Object, Long> ffOps = client.fireAndForgetOps();

        Set<Long> syncIds = new HashSet<Long>();

        syncIds.add(ffOps.insert(SPACE_ID, Arrays.asList(10, "10")));
        syncIds.add(ffOps.delete(SPACE_ID, Collections.singletonList(10)));

        syncIds.add(ffOps.insert(SPACE_ID, Arrays.asList(10, "10")));
        syncIds.add(ffOps.update(SPACE_ID, Collections.singletonList(10), Arrays.asList("=", 1, "ten")));

        syncIds.add(ffOps.replace(SPACE_ID, Arrays.asList(20, "20")));
        syncIds.add(ffOps.upsert(SPACE_ID, Collections.singletonList(20), Arrays.asList(20, "twenty"),
            Arrays.asList("=", 1, "twenty")));

        syncIds.add(ffOps.insert(SPACE_ID, Arrays.asList(30, "30")));
        syncIds.add(ffOps.call("box.space.basic_test:delete", Collections.singletonList(30)));

        // Check the syncs.
        assertFalse(syncIds.contains(0L));
        assertEquals(8, syncIds.size());

        // The reply for synchronous ping will
        // indicate to us that previous fire & forget operations are completed.
        client.syncOps().ping();

        // Check the effects
        checkTupleResult(consoleSelect(SPACE_NAME, 10), Arrays.asList(10, "ten"));
        checkTupleResult(consoleSelect(SPACE_NAME, 20), Arrays.asList(20, "twenty"));
        assertEquals(consoleSelect(SPACE_NAME, 30), Collections.emptyList());
    }
}
