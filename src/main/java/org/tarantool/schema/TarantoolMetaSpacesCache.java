package org.tarantool.schema;

import org.tarantool.Iterator;
import org.tarantool.TarantoolClientImpl;
import org.tarantool.TarantoolClientOps;
import org.tarantool.util.TupleTwo;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * In-memory schema cache.
 * <p>
 * Caches meta spaces {@code _vspace} and {@code _vindex}.
 * <p>
 * This class is not a part of public API.
 */
public class TarantoolMetaSpacesCache implements TarantoolSchemaMeta {

    private static final int VSPACE_ID = 281;
    private static final int VSPACE_ID_INDEX_ID = 0;

    private static final int VINDEX_ID = 289;
    private static final int VINDEX_ID_INDEX_ID = 0;

    /**
     * Describes the theoretical maximum tuple size
     * which is (2^31 - 1) (box.schema.SPACE_MAX)
     */
    private static final int MAX_TUPLES = 2_147_483_647;

    private TarantoolClientImpl client;

    private volatile Map<String, TarantoolSpaceMeta> cachedSpaces = Collections.emptyMap();
    private volatile long schemaVersion;

    public TarantoolMetaSpacesCache(TarantoolClientImpl client) {
        this.client = client;
    }

    @Override
    public TarantoolSpaceMeta getSpace(String spaceName) {
        TarantoolSpaceMeta space = cachedSpaces.get(spaceName);
        if (space == null) {
            throw new TarantoolSpaceNotFoundException(spaceName);
        }
        return space;
    }

    @Override
    public TarantoolIndexMeta getSpaceIndex(String spaceName, String indexName) {
        TarantoolIndexMeta index = getSpace(spaceName).getIndex(indexName);
        if (index == null) {
            throw new TarantoolIndexNotFoundException(spaceName, indexName);
        }
        return index;
    }

    @Override
    public long getSchemaVersion() {
        return schemaVersion;
    }

    @Override
    public synchronized long refresh() {
        TupleTwo<List<TarantoolSpaceMeta>, Long> result = fetchSpaces();
        cachedSpaces = result.getFirst()
            .stream()
            .collect(
                Collectors.toConcurrentMap(
                    TarantoolSpaceMeta::getName,
                    Function.identity(),
                    (oldValue, newValue) -> newValue,
                    ConcurrentHashMap::new
                )
            );
        return schemaVersion = result.getSecond();
    }

    @Override
    public boolean isInitialized() {
        return schemaVersion != 0;
    }

    private TupleTwo<List<TarantoolSpaceMeta>, Long> fetchSpaces() {
        TarantoolClientOps<Integer, List<?>, Object, TupleTwo<List<?>, Long>> clientOps = client.unsafeSchemaOps();

        long firstRequestSchema = -1;
        long secondRequestSchema = 0;
        List<?> spaces = null;
        List<?> indexes = null;
        while (firstRequestSchema != secondRequestSchema) {
            TupleTwo<List<?>, Long> spacesResult = clientOps
                .select(VSPACE_ID, VSPACE_ID_INDEX_ID, Collections.emptyList(), 0, Integer.MAX_VALUE, Iterator.ALL);
            TupleTwo<List<?>, Long> indexesResult = clientOps
                .select(VINDEX_ID, VINDEX_ID_INDEX_ID, Collections.emptyList(), 0, Integer.MAX_VALUE, Iterator.ALL);
            spaces = spacesResult.getFirst();
            indexes = indexesResult.getFirst();
            firstRequestSchema = spacesResult.getSecond();
            secondRequestSchema = indexesResult.getSecond();
        }

        Map<Integer, List<List<?>>> indexesBySpace = indexes.stream()
            .map(tuple -> (List<?>) tuple)
            .collect(Collectors.groupingBy(tuple -> (Integer) tuple.get(0)));

        List<TarantoolSpaceMeta> cachedMeta = spaces.stream()
            .map(tuple -> (List<?>) tuple)
            .map(tuple -> TarantoolSpaceMeta.fromTuple(
                tuple,
                indexesBySpace.getOrDefault((Integer) tuple.get(0), Collections.emptyList()))
            )
            .collect(Collectors.toList());

        return TupleTwo.of(cachedMeta, firstRequestSchema);
    }

}
