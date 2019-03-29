package org.tarantool;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Tests operations available in {@link TarantoolSQLOps} interface.
 */
public abstract class AbstractTarantoolSQLOpsIT extends AbstractTarantoolSQLConnectorIT {

    protected abstract TarantoolSQLOps<Object, Long, List<Map<String, Object>>> getSQLOps();

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
