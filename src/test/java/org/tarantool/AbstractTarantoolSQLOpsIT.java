package org.tarantool;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.tarantool.TestAssumptions.assumeMinimalServerVersion;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

/**
 * Tests operations available in {@link TarantoolSQLOps} interface.
 */
public abstract class AbstractTarantoolSQLOpsIT extends AbstractTarantoolSQLConnectorIT {

    private static final String[] SETUP_SCRIPT = new String[] {
        "\\set language sql",

        "CREATE TABLE sql_test (id INTEGER PRIMARY KEY, val VARCHAR(100));",
        "CREATE UNIQUE INDEX sql_test_val_index_unique ON sql_test (val);",

        "INSERT INTO sql_test VALUES (1, 'A');",
        "INSERT INTO sql_test VALUES (2, 'B');",
        "INSERT INTO sql_test VALUES (3, 'C');"
    };

    private static final String[] CLEAN_SCRIPT = new String[] {
        "DROP TABLE sql_test;",
        "\\set language lua"
    };

    protected abstract TarantoolSQLOps<Object, Long, List<Map<String, Object>>> getSQLOps();

    @BeforeEach
    void setUpTest() {
        assumeMinimalServerVersion(console, ServerVersion.V_2_1);
        executeLua(SETUP_SCRIPT);
    }

    @AfterEach
    void tearDownTest() {
        executeLua(CLEAN_SCRIPT);
    }

    @Test
    public void testSelectOne() {
        List<Map<String, Object>> result = getSQLOps().query("SELECT id, val FROM sql_test WHERE id = 1");
        checkTupleResult(
                asResult(new Object[][] { {"ID", 1, "VAL", "A"} }),
                result
        );
    }

    @Test
    public void testSelectMany() {
        List<Map<String, Object>> result = getSQLOps().query("SELECT id, val FROM sql_test WHERE id = 1 or val = 'B'");
        checkTupleResult(
                asResult(new Object[][] { {"ID", 1, "VAL", "A"}, {"ID", 2, "VAL", "B"} }),
                result
        );
    }

    @Test
    public void testSelectEmpty() {
        List<Map<String, Object>> result = getSQLOps().query("SELECT id, val FROM sql_test WHERE val = 'AB'");
        checkTupleResult(
                asResult(new Object[][] { }),
                result
        );
    }

    @Test
    public void testInsertOneRecord() {
        Long rowsAffected = getSQLOps().update("INSERT INTO sql_test VALUES (27, 'Z');");
        assertEquals(1L, (long) rowsAffected);
    }

    @Test
    public void testInsertDuplication() {
        assertThrows(TarantoolException.class, () -> getSQLOps().update("INSERT INTO sql_test VALUES (1, 'A');"));
    }

}
