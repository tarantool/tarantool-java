package org.tarantool;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.tarantool.TestAssumptions.assumeMinimalServerVersion;
import static org.tarantool.TestUtils.makeDefaultClientConfig;
import static org.tarantool.TestUtils.makeTestClient;
import static org.tarantool.TestUtils.openConnection;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

/**
 * Tests operations available in {@link TarantoolSQLOps} interface.
 *
 * NOTE: Parametrized tests can be simplified after
 * https://github.com/junit-team/junit5/issues/878
 */
class TarantoolSQLOpsIT {

    private static final int RESTART_TIMEOUT = 2000;

    private static TarantoolTestHelper testHelper;

    private static final String[] SETUP_SCRIPT = new String[] {
        "CREATE TABLE sql_test (id INTEGER PRIMARY KEY, val VARCHAR(100));",
        "CREATE UNIQUE INDEX sql_test_val_index_unique ON sql_test (val);",

        "INSERT INTO sql_test VALUES (1, 'A');",
        "INSERT INTO sql_test VALUES (2, 'B');",
        "INSERT INTO sql_test VALUES (3, 'C');"
    };

    private static final String[] CLEAN_SCRIPT = new String[] {
        "DROP TABLE sql_test;",
    };

    static Stream<SqlOpsProvider> getSQLOps() {
        return Stream.of(new ClientSqlOpsProvider(), new ConnectionSqlOpsProvider());
    }

    @BeforeAll
    static void setupEnv() {
        testHelper = new TarantoolTestHelper("sql-ops-it");
        testHelper.createInstance();
        testHelper.startInstance();
    }

    @AfterAll
    static void cleanupEnv() {
        testHelper.stopInstance();
    }

    @BeforeEach
    void setUpTest() {
        assumeMinimalServerVersion(testHelper.getInstanceVersion(), ServerVersion.V_2_1);
        testHelper.executeSql(SETUP_SCRIPT);
    }

    @AfterEach
    void tearDownTest() {
        testHelper.executeSql(CLEAN_SCRIPT);
    }

    @ParameterizedTest
    @MethodSource("getSQLOps")
    void testSelectOne(SqlOpsProvider provider) {
        List<Map<String, Object>> result = provider.getSqlOps()
            .query("SELECT id, val FROM sql_test WHERE id = 1");
        checkTupleResult(
                asResult(new Object[][] { {"ID", 1, "VAL", "A"} }),
                result
        );

        provider.close();
    }

    @ParameterizedTest
    @MethodSource("getSQLOps")
    void testSelectMany(SqlOpsProvider provider) {
        List<Map<String, Object>> result = provider.getSqlOps()
            .query("SELECT id, val FROM sql_test WHERE id = 1 or val = 'B'");
        checkTupleResult(
                asResult(new Object[][] { {"ID", 1, "VAL", "A"}, {"ID", 2, "VAL", "B"} }),
                result
        );

        provider.close();
    }

    @ParameterizedTest
    @MethodSource("getSQLOps")
    void testSelectEmpty(SqlOpsProvider provider) {
        List<Map<String, Object>> result = provider.getSqlOps()
            .query("SELECT id, val FROM sql_test WHERE val = 'AB'");
        checkTupleResult(asResult(new Object[][] { }), result);

        provider.close();
    }

    @ParameterizedTest
    @MethodSource("getSQLOps")
    void testInsertOneRecord(SqlOpsProvider provider) {
        Long rowsAffected = provider.getSqlOps()
            .update("INSERT INTO sql_test VALUES (27, 'Z');");
        assertEquals(1L, (long) rowsAffected);

        provider.close();
    }

    @ParameterizedTest
    @MethodSource("getSQLOps")
    void testInsertDuplication(SqlOpsProvider provider) {
        assertThrows(TarantoolException.class, () -> provider.getSqlOps()
            .update("INSERT INTO sql_test VALUES (1, 'A');"));

        provider.close();
    }

    private void checkTupleResult(List<Map<String, Object>> expected, List<Map<String, Object>> actual) {
        assertNotNull(expected);
        assertEquals(expected, actual);
    }

    private List<Map<String, Object>> asResult(Object[][] tuples) {
        List<Map<String, Object>> result = new ArrayList<>();
        if (tuples != null) {
            for (int i = 0; i < tuples.length; i++) {
                Object[] tuple = tuples[i];
                if (tuple.length % 2 != 0) {
                    continue;
                }
                Map<String, Object> row = new HashMap<>();
                for (int j = 0; j <= tuple.length / 2; j += 2) {
                    row.put(tuple[j].toString(), tuple[j + 1]);
                }
                result.add(row);
            }
        }
        return result;
    }

    private static TarantoolConnection makeConnection() {
        return openConnection(
            TarantoolTestHelper.HOST,
            TarantoolTestHelper.PORT,
            TarantoolTestHelper.USERNAME,
            TarantoolTestHelper.PASSWORD
        );
    }

    private interface SqlOpsProvider {
        TarantoolSQLOps<Object, Long, List<Map<String, Object>>> getSqlOps();

        void close();
    }

    private static class ClientSqlOpsProvider implements SqlOpsProvider {

        private TarantoolClient client = makeTestClient(makeDefaultClientConfig(), RESTART_TIMEOUT);

        @Override
        public TarantoolSQLOps<Object, Long, List<Map<String, Object>>> getSqlOps() {
            return client.sqlSyncOps();
        }

        @Override
        public void close() {
            client.close();
        }

    }

    private static class ConnectionSqlOpsProvider implements SqlOpsProvider {

        private TarantoolConnection connection = makeConnection();

        @Override
        public TarantoolSQLOps<Object, Long, List<Map<String, Object>>> getSqlOps() {
            return connection;
        }

        @Override
        public void close() {
            connection.close();
        }

    }
}
