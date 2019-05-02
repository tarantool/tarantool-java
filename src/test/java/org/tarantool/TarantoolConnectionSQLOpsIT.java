package org.tarantool;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;

import java.util.List;
import java.util.Map;

/**
 * Tests for synchronous operations of {@link TarantoolConnection} implementation.
 * <p>
 * Actual tests reside in base class.
 */
public class TarantoolConnectionSQLOpsIT extends AbstractTarantoolSQLOpsIT {

    private TarantoolConnection connection;

    @BeforeEach
    public void setup() {
        connection = TestUtils.openConnection(HOST, PORT, USERNAME, PASSWORD);
    }

    @AfterEach
    public void tearDown() {
        connection.close();
    }

    @Override
    protected TarantoolSQLOps<Object, Long, List<Map<String, Object>>> getSQLOps() {
        return connection;
    }

}
