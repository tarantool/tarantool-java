package org.tarantool.schema;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.fail;
import static org.tarantool.TestUtils.makeDefaultClientConfig;

import org.tarantool.ServerVersion;
import org.tarantool.TarantoolClientConfig;
import org.tarantool.TarantoolClientImpl;
import org.tarantool.TarantoolTestHelper;
import org.tarantool.TestAssumptions;
import org.tarantool.schema.TarantoolIndexMeta.IndexOptions;
import org.tarantool.schema.TarantoolIndexMeta.IndexPart;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;

@DisplayName("A schema meta")
public class ClientSchemaIT {

    private static TarantoolTestHelper testHelper;

    private TarantoolClientImpl client;

    @BeforeAll
    public static void setupEnv() {
        testHelper = new TarantoolTestHelper("client-schema-it");
        testHelper.createInstance();
        testHelper.startInstance();
    }

    @AfterAll
    public static void teardownEnv() {
        testHelper.stopInstance();
    }

    @BeforeEach
    public void setup() {
        TarantoolClientConfig config = makeDefaultClientConfig();

        client = new TarantoolClientImpl(
            TarantoolTestHelper.HOST + ":" + TarantoolTestHelper.PORT,
            config
        );
    }

    @AfterEach
    public void tearDown() {
        client.close();
        testHelper.executeLua("box.space.count_space and box.space.count_space:drop()");
    }

    @Test
    @DisplayName("fetched a space with its index")
    void testFetchSpaces() {
        testHelper.executeLua(
            "box.schema.space.create('count_space', { format = " +
                "{ {name = 'id', type = 'integer'}," +
                "  {name = 'counts', type = 'integer'} }" +
                "})"
        );
        testHelper.executeLua("box.space.count_space:create_index('pk', { type = 'TREE', parts = {'id'} } )");

        TarantoolSchemaMeta meta = new TarantoolMetaSpacesCache(client);
        meta.refresh();

        TarantoolSpaceMeta space = meta.getSpace("count_space");
        assertNotNull(space);
        assertEquals("count_space", space.getName());

        List<TarantoolSpaceMeta.SpaceField> spaceFormat = space.getFormat();
        assertEquals(2, spaceFormat.size());
        assertEquals("id", spaceFormat.get(0).getName());
        assertEquals("integer", spaceFormat.get(0).getType());
        assertEquals("counts", spaceFormat.get(1).getName());
        assertEquals("integer", spaceFormat.get(1).getType());

        TarantoolIndexMeta primaryIndex = space.getIndex("pk");
        TarantoolIndexMeta expectedPrimaryIndex = new TarantoolIndexMeta(
            0, "pk", "TREE",
            new IndexOptions(true),
            Collections.singletonList(new IndexPart(0, "integer"))
        );
        assertIndex(expectedPrimaryIndex, primaryIndex);
    }

    @Test
    @DisplayName("fetched newly created spaces and indexes")
    void testFetchNewSpaces() {
        // add count_space
        testHelper.executeLua(
            "box.schema.space.create('count_space', { format = " +
                "{ {name = 'id', type = 'integer'}," +
                "  {name = 'counts', type = 'integer'} }" +
                "})"
        );
        TarantoolSchemaMeta meta = new TarantoolMetaSpacesCache(client);
        meta.refresh();
        TarantoolSpaceMeta space = meta.getSpace("count_space");
        assertNotNull(space);
        assertEquals("count_space", space.getName());
        assertThrows(TarantoolSpaceNotFoundException.class, () -> meta.getSpace("count_space_2"));

        // add count_space_2
        testHelper.executeLua(
            "box.schema.space.create('count_space_2', { format = " +
                "{ {name = 'id', type = 'integer'} } })"
        );
        meta.refresh();
        space = meta.getSpace("count_space_2");
        assertNotNull(space);
        assertEquals("count_space_2", space.getName());
        assertThrows(TarantoolIndexNotFoundException.class, () -> meta.getSpaceIndex("count_space_2", "pk"));

        // add a primary index for count_space_2
        testHelper.executeLua(
            "box.space.count_space_2:create_index('pk', { unique = true, type = 'TREE', parts = {'id'} } )"
        );
        meta.refresh();
        TarantoolIndexMeta spaceIndex = meta.getSpaceIndex("count_space_2", "pk");
        TarantoolIndexMeta expectedPrimaryIndex = new TarantoolIndexMeta(
            0, "pk", "TREE",
            new IndexOptions(true),
            Collections.singletonList(new IndexPart(0, "integer"))
        );
        assertIndex(expectedPrimaryIndex, spaceIndex);
    }

    @Test
    @DisplayName("fetched space indexes of a space")
    void testFetchIndexes() {
        testHelper.executeLua(
            "box.schema.space.create('count_space', { format = " +
                "{ {name = 'id', type = 'integer'}," +
                "  {name = 'counts', type = 'integer'} }" +
                "})"
        );
        testHelper.executeLua(
            "box.space.count_space:create_index('pk', { type = 'HASH', parts = {'id'} } )",
            "box.space.count_space:create_index('c_index', { unique = false, type = 'TREE', parts = {'counts'} } )"
        );

        TarantoolSchemaMeta meta = new TarantoolMetaSpacesCache(client);
        meta.refresh();

        TarantoolIndexMeta primaryIndex = meta.getSpaceIndex("count_space", "pk");
        TarantoolIndexMeta expectedPrimaryIndex = new TarantoolIndexMeta(
            0, "pk", "HASH",
            new IndexOptions(true),
            Collections.singletonList(new IndexPart(0, "integer"))
        );
        assertIndex(expectedPrimaryIndex, primaryIndex);

        TarantoolIndexMeta secondaryIndex = meta.getSpaceIndex("count_space", "c_index");
        TarantoolIndexMeta expectedSecondaryIndex = new TarantoolIndexMeta(
            1, "c_index", "TREE",
            new IndexOptions(false),
            Collections.singletonList(new IndexPart(1, "integer"))
        );
        assertIndex(expectedSecondaryIndex, secondaryIndex);
    }

    @Test
    @DisplayName("fetched sql table primary index")
    void testFetchSqlIndexes() {
        TestAssumptions.assumeMinimalServerVersion(testHelper.getInstanceVersion(), ServerVersion.V_2_1);
        testHelper.executeSql("create table my_table (id int primary key, val varchar(100))");

        TarantoolSchemaMeta meta = new TarantoolMetaSpacesCache(client);
        meta.refresh();

        TarantoolIndexMeta primaryIndex = meta.getSpaceIndex("MY_TABLE", "pk_unnamed_MY_TABLE_1");
        TarantoolIndexMeta expectedPrimaryIndex = new TarantoolIndexMeta(
            0, "pk_unnamed_MY_TABLE_1", "tree",
            new IndexOptions(true),
            Collections.singletonList(new IndexPart(0, "integer"))
        );
        assertIndex(expectedPrimaryIndex, primaryIndex);
    }

    @Test
    @DisplayName("got an error with a wrong space name")
    void tesGetUnknownSpace() {
        TarantoolSchemaMeta meta = new TarantoolMetaSpacesCache(client);
        meta.refresh();

        TarantoolSpaceNotFoundException exception = assertThrows(
            TarantoolSpaceNotFoundException.class,
            () -> meta.getSpace("unknown_space")
        );
        assertEquals("unknown_space", exception.getSchemaName());
    }

    @Test
    @DisplayName("got an error with a wrong space index name")
    void testGetUnknownSpaceIndex() {
        testHelper.executeLua(
            "box.schema.space.create('count_space', { format = " +
                "{ {name = 'id', type = 'integer'} } })"
        );
        testHelper.executeLua("box.space.count_space:create_index('pk', { type = 'TREE', parts = {'id'} } )");

        TarantoolSchemaMeta meta = new TarantoolMetaSpacesCache(client);
        meta.refresh();

        assertEquals("count_space", meta.getSpace("count_space").getName());
        TarantoolIndexNotFoundException exception = assertThrows(
            TarantoolIndexNotFoundException.class,
            () -> meta.getSpaceIndex("count_space", "wrong_pk")
        );
        assertEquals("wrong_pk", exception.getIndexName());
    }

    private void assertIndex(TarantoolIndexMeta expectedIndex, TarantoolIndexMeta actualIndex) {
        assertEquals(expectedIndex.getId(), actualIndex.getId());
        assertEquals(expectedIndex.getName(), actualIndex.getName());
        assertEquals(expectedIndex.getType(), actualIndex.getType());
        assertEqualsOptions(expectedIndex.getOptions(), actualIndex.getOptions());
        assertEqualsParts(expectedIndex.getParts(), actualIndex.getParts());
    }

    private void assertEqualsOptions(IndexOptions expected, IndexOptions actual) {
        assertEquals(expected.isUnique(), actual.isUnique());
    }

    private void assertEqualsParts(List<IndexPart> expected, List<IndexPart> actual) {
        if (expected.size() != actual.size()) {
            fail("Part lists have different sizes");
        }
        for (int i = 0; i < expected.size(); i++) {
            IndexPart expectedPart = expected.get(i);
            IndexPart actualPart = actual.get(i);
            assertEquals(expectedPart.getFieldNumber(), actualPart.getFieldNumber());
            assertEquals(expectedPart.getType(), actualPart.getType());
        }
    }

}
