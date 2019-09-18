package org.tarantool.schema;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Keeps a space metadata.
 */
public class TarantoolSpaceMeta {

    public static final int VSPACE_ID_FIELD_NUMBER = 0;
    public static final int VSPACE_NAME_FIELD_NUMBER = 2;
    public static final int VSPACE_ENGINE_FIELD_NUMBER = 3;
    public static final int VSPACE_FORMAT_FIELD_NUMBER = 6;

    private final int id;
    private final String name;
    private final String engine;
    private final List<SpaceField> format;
    private final Map<String, TarantoolIndexMeta> indexes;

    public static TarantoolSpaceMeta fromTuple(List<?> spaceTuple, List<List<?>> indexTuples) {
        List<SpaceField> fields = ((List<Map<String, ?>>) spaceTuple.get(VSPACE_FORMAT_FIELD_NUMBER)).stream()
            .map(field -> new SpaceField(field.get("name").toString(), field.get("type").toString()))
            .collect(Collectors.toList());

        Map<String, TarantoolIndexMeta> indexesMap = indexTuples.stream()
            .map(TarantoolIndexMeta::fromTuple)
            .collect(Collectors.toMap(TarantoolIndexMeta::getName, Function.identity()));

        return new TarantoolSpaceMeta(
            (Integer) spaceTuple.get(VSPACE_ID_FIELD_NUMBER),
            spaceTuple.get(VSPACE_NAME_FIELD_NUMBER).toString(),
            spaceTuple.get(VSPACE_ENGINE_FIELD_NUMBER).toString(),
            Collections.unmodifiableList(fields),
            Collections.unmodifiableMap(indexesMap)
        );
    }

    public TarantoolSpaceMeta(int id,
                              String name,
                              String engine,
                              List<SpaceField> format,
                              Map<String, TarantoolIndexMeta> indexes) {
        this.id = id;
        this.name = name;
        this.engine = engine;
        this.format = format;
        this.indexes = indexes;
    }

    public int getId() {
        return id;
    }

    public String getName() {
        return name;
    }

    public String getEngine() {
        return engine;
    }

    public List<SpaceField> getFormat() {
        return format;
    }

    public Map<String, TarantoolIndexMeta> getIndexes() {
        return indexes;
    }

    public TarantoolIndexMeta getIndex(String indexName) {
        return indexes.get(indexName);
    }

    public static class SpaceField {

        private final String name;
        private final String type;

        public SpaceField(String name, String type) {
            this.name = name;
            this.type = type;
        }

        public String getName() {
            return name;
        }

        public String getType() {
            return type;
        }

    }

}
