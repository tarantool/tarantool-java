package org.tarantool;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Tests operations available in {@link TarantoolClientOps} interface.
 */
public abstract class AbstractTarantoolOpsIT extends AbstractTarantoolConnectorIT {
    protected abstract TarantoolClientOps<Integer, List<?>, Object, List<?>> getOps();

    @Test
    public void testSelectOne() {
        List<?> res = getOps().select(SPACE_ID, PK_INDEX_ID, Collections.singletonList(1), 0, 1, Iterator.EQ);
        checkTupleResult(res, Arrays.asList(1, "one"));
    }

    @Test
    public void testSelectMany() {
        List<?> res = getOps().select(SPACE_ID, PK_INDEX_ID, Collections.singletonList(10), 0, 10,
            Iterator.LT);

        assertNotNull(res);
        assertEquals(3, res.size());

        // Descending order.
        assertEquals(Arrays.asList(3, "three"), res.get(0));
        assertEquals(Arrays.asList(2, "two"), res.get(1));
        assertEquals(Arrays.asList(1, "one"), res.get(2));
    }

    @Test
    public void testSelectOffsetLimit() {
        List<?> res = getOps().select(SPACE_ID, PK_INDEX_ID, Collections.singletonList(10), 1, 1, Iterator.LT);
        assertNotNull(res);
        assertEquals(1, res.size());

        assertEquals(Arrays.asList(2, "two"), res.get(0));
    }

    @Test
    public void testSelectUsingSecondaryIndex() {
        List<?> res = getOps().select(SPACE_ID, VIDX_INDEX_ID, Collections.singletonList("one"), 0, 1, Iterator.EQ);
        checkTupleResult(res, Arrays.asList(1, "one"));
    }

    @Test
    public void testInsertSimple() {
        List tup = Arrays.asList(100, "hundred");
        List<?> res = getOps().insert(SPACE_ID, tup);

        checkTupleResult(res, tup);

        // Check it actually was inserted.
        checkTupleResult(consoleSelect(SPACE_NAME, 100), tup);
    }

    @Test
    public void testInsertMultiPart() {
        List tup = Arrays.asList(100, "hundred", "h u n d r e d");
        List<?> res = getOps().insert(MULTI_PART_SPACE_ID, tup);

        checkTupleResult(res, tup);

        // Check it actually was inserted.
        checkTupleResult(consoleSelect(MULTIPART_SPACE_NAME, Arrays.asList(100, "hundred")), tup);
    }

    @Test
    public void testReplaceSimple() {
        checkReplace(SPACE_NAME,
            SPACE_ID,
            Collections.singletonList(10),
            Arrays.asList(10, "10"),
            Arrays.asList(10, "ten"));
    }

    @Test
    public void testReplaceMultiPartKey() {
        checkReplace(MULTIPART_SPACE_NAME,
            MULTI_PART_SPACE_ID,
            Arrays.asList(10, "10"),
            Arrays.asList(10, "10", "10"),
            Arrays.asList(10, "10", "ten"));
    }

    private void checkReplace(String space, int spaceId, List key, List createTuple, List updateTuple) {
        List<?> res = getOps().replace(spaceId, createTuple);
        checkTupleResult(res, createTuple);

        // Check it actually was created.
        checkTupleResult(consoleSelect(space, key), createTuple);

        // Update
        res = getOps().replace(spaceId, updateTuple);
        checkTupleResult(res, updateTuple);

        // Check it actually was updated.
        checkTupleResult(consoleSelect(space, key), updateTuple);
    }

    @Test
    public void testUpdateNonExistingHasNoEffect() {
        List op0 = Arrays.asList("=", 3, "trez");

        List res = getOps().update(SPACE_ID, Collections.singletonList(30), op0);

        assertNotNull(res);
        assertEquals(0, res.size());

        // Check it doesn't exist.
        res = getOps().select(SPACE_ID, PK_INDEX_ID, Collections.singletonList(30), 0, 1, Iterator.EQ);
        assertNotNull(res);
        assertEquals(0, res.size());
    }

    @Test
    public void testUpdate() {
        checkUpdate(SPACE_NAME,
            SPACE_ID,
            // key
            Collections.singletonList(30),
            // init tuple
            Arrays.asList(30, "30"),
            // expected tuple
            Arrays.asList(30, "thirty"),
            // operations
            Arrays.asList("!", 1, "thirty"),
            Arrays.asList("#", 2, 1));
    }

    @Test
    public void testUpdateMultiPart() {
        checkUpdate(MULTIPART_SPACE_NAME,
            MULTI_PART_SPACE_ID,
            Arrays.asList(30, "30"),
            Arrays.asList(30, "30", "30"),
            Arrays.asList(30, "30", "thirty"),
            Arrays.asList("=", 2, "thirty"));
    }

    private void checkUpdate(String space, int spaceId, List key, List initTuple, List expectedTuple,
        Object ... ops) {
        // Try update non-existing key.
        List<?> res = getOps().update(spaceId, key, ops);
        assertNotNull(res);
        assertEquals(0, res.size());

        // Check it still doesn't exists.
        assertEquals(Collections.emptyList(), consoleSelect(space, key));

        // Create the tuple.
        res = getOps().insert(spaceId, initTuple);
        checkTupleResult(res, initTuple);

        // Apply the update operations.
        res = getOps().update(spaceId, key, ops);
        checkTupleResult(res, expectedTuple);

        // Check that update was actually performed.
        checkTupleResult(consoleSelect(space, key), expectedTuple);
    }

    @Test
    public void testUpsertSimple() {
        checkUpsert(SPACE_NAME,
            SPACE_ID,
            Collections.singletonList(40),
            Arrays.asList(40, "40"),
            Arrays.asList(40, "fourty"),
            Arrays.asList("=", 1, "fourty"));
    }

    @Test
    public void testUpsertMultiPart() {
        checkUpsert(MULTIPART_SPACE_NAME,
            MULTI_PART_SPACE_ID,
            Arrays.asList(40, "40"),
            Arrays.asList(40, "40", "40"),
            Arrays.asList(40, "40", "fourty"),
            Arrays.asList("=", 2, "fourty"));
    }

    private void checkUpsert(String space, int spaceId, List key, List defTuple, List expectedTuple,
        Object ... ops) {
        // Check that key doesn't exist.
        assertEquals(Collections.emptyList(), consoleSelect(space, key));

        // Try upsert non-existing key.
        List<?> res = getOps().upsert(spaceId, key, defTuple, ops);
        assertNotNull(res);
        assertEquals(0, res.size());

        // Check that default tuple was inserted.
        checkTupleResult(consoleSelect(space, key), defTuple);

        // Apply the operations.
        res = getOps().upsert(spaceId, key, defTuple, ops);
        assertNotNull(res);
        assertEquals(0, res.size());

        // Check that update was actually performed.
        checkTupleResult(consoleSelect(space, key), expectedTuple);
    }

    @Test
    public void testDeleteSimple() {
        checkDelete(SPACE_NAME,
            SPACE_ID,
            Collections.singletonList(50),
            Arrays.asList(50, "fifty"));
    }

    @Test
    public void testDeleteMultiPartKey() {
        checkDelete(MULTIPART_SPACE_NAME,
            MULTI_PART_SPACE_ID,
            Arrays.asList(50, "50"),
            Arrays.asList(50, "50", "fifty"));
    }

    private void checkDelete(String space, int spaceId, List key, List tuple) {
        // Check the key doesn't exists.
        assertEquals(Collections.emptyList(), consoleSelect(space, key));

        // Try to delete non-existing key.
        List<?> res = getOps().delete(spaceId, key);
        assertNotNull(res);
        assertEquals(0, res.size());

        // Create tuple.
        res = getOps().insert(spaceId, tuple);
        checkTupleResult(res, tuple);

        // Check the tuple was created.
        checkTupleResult(consoleSelect(space, key), tuple);

        // Delete it.
        res = getOps().delete(spaceId, key);
        checkTupleResult(res, tuple);

        // Check it actually was deleted.
        assertEquals(Collections.emptyList(), consoleSelect(space, key));
    }

    @Test
    public void testEval() {
        assertEquals(Collections.singletonList("true"), getOps().eval("return echo(...)", "true"));
    }

    @Test
    public void testCall() {
        assertEquals(Collections.singletonList(Collections.singletonList("true")), getOps().call("echo", "true"));
    }

    @Test
    public void testPing() {
        getOps().ping();
    }

    @Test
    public void testDeleteFromNonExistingSpace() {
        TarantoolException ex = assertThrows(TarantoolException.class, new Executable() {
            @Override
            public void execute() throws Throwable {
                getOps().delete(5555, Collections.singletonList(2));
            }
        });
        assertEquals("Space '5555' does not exist", ex.getMessage());
    }

    @Test
    public void testSelectUnsupportedIterator() {
        TarantoolException ex = assertThrows(TarantoolException.class, new Executable() {
            @Override
            public void execute() throws Throwable {
                getOps().select(SPACE_ID, PK_INDEX_ID, Collections.singletonList(1), 0, 1, Iterator.OVERLAPS);
            }
        });
        assertEquals(
            "Index 'pk' (TREE) of space 'basic_test' (memtx) does not support requested iterator type",
            ex.getMessage());
    }

    @Test
    public void testSelectNonExistingKey() {
        List<?> res = getOps().select(SPACE_ID, PK_INDEX_ID, Collections.singletonList(5555), 0, 1,
            Iterator.EQ);

        assertNotNull(res);
        assertEquals(0, res.size());
    }

    @Test
    public void testSelectFromNonExistingIndex() {
        TarantoolException ex = assertThrows(TarantoolException.class, new Executable() {
            @Override
            public void execute() throws Throwable {
                getOps().select(SPACE_ID, 5555, Collections.singletonList(2), 0, 1, Iterator.EQ);
            }
        });
        assertEquals("No index #5555 is defined in space 'basic_test'", ex.getMessage());
    }

    @Test
    public void testSelectFromNonExistingSpace() {
        TarantoolException ex = assertThrows(TarantoolException.class, new Executable() {
            @Override
            public void execute() throws Throwable {
                getOps().select(5555, 0, Collections.singletonList(5555), 0, 1, Iterator.EQ);
            }
        });
        assertEquals("Space '5555' does not exist", ex.getMessage());
    }

    @Test
    public void testInsertDuplicateKey() {
        final List tup = Arrays.asList(1, "uno");
        TarantoolException ex = assertThrows(TarantoolException.class, new Executable() {
            @Override
            public void execute() throws Throwable {
                getOps().insert(SPACE_ID, tup);
            }
        });
        assertEquals("Duplicate key exists in unique index 'pk' in space 'basic_test'", ex.getMessage());

        // Check the tuple stayed intact.
        checkTupleResult(consoleSelect(SPACE_NAME, 1), Arrays.asList(1, "one"));
    }

    @Test
    public void testInsertToNonExistingSpace() {
        TarantoolException ex = assertThrows(TarantoolException.class, new Executable() {
            @Override
            public void execute() throws Throwable {
                getOps().insert(5555, Arrays.asList(1, "one"));
            }
        });
        assertEquals("Space '5555' does not exist", ex.getMessage());
    }

    @Test
    public void testInsertInvalidData() {
        // Invalid types.
        TarantoolException ex = assertThrows(TarantoolException.class, new Executable() {
            @Override
            public void execute() throws Throwable {
                getOps().insert(SPACE_ID, Arrays.asList("one", 1));
            }
        });
        assertEquals("Tuple field 1 type does not match one required by operation: expected integer", ex.getMessage());

        // Invalid tuple size.
        ex = assertThrows(TarantoolException.class, new Executable() {
            @Override
            public void execute() throws Throwable {
                getOps().insert(SPACE_ID, Collections.singletonList("one"));
            }
        });
        assertEquals("Tuple field count 1 is less than required by space format or defined indexes " +
            "(expected at least 2)", ex.getMessage());
    }
}
