package org.tarantool;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;

import java.util.List;
import java.util.Map;

/**
 * Tests for synchronous operations of {@link TarantoolClientImpl#sqlSyncOps()} implementation.
 * <p>
 * Actual tests reside in base class.
 */
public class SQLOperationsIT extends AbstractTarantoolSQLOpsIT {

    private TarantoolClient client;

    @BeforeEach
    public void setup() {
        client = makeClient();
    }

    @AfterEach
    public void tearDown() {
        if (client != null) {
            client.close();
        }
    }

    @Override
    protected TarantoolSQLOps<Object, Long, List<Map<String, Object>>> getSQLOps() {
        return client.sqlSyncOps();
    }

}
