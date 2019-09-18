package org.tarantool.schema;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Keeps a space index metadata.
 */
public class TarantoolIndexMeta {

    public static final int VINDEX_IID_FIELD_NUMBER = 1;
    public static final int VINDEX_NAME_FIELD_NUMBER = 2;
    public static final int VINDEX_TYPE_FIELD_NUMBER = 3;
    public static final int VINDEX_OPTIONS_FIELD_NUMBER = 4;
    public static final int VINDEX_PARTS_FIELD_NUMBER = 5;

    public static final int VINDEX_PART_FIELD = 0;
    public static final int VINDEX_PART_TYPE = 1;

    private final int id;
    private final String name;
    private final String type;
    private final IndexOptions options;
    private final List<IndexPart> parts;

    public TarantoolIndexMeta(int id,
                              String name,
                              String type,
                              IndexOptions options,
                              List<IndexPart> parts) {
        this.id = id;
        this.name = name;
        this.type = type;
        this.options = options;
        this.parts = parts;
    }

    public static TarantoolIndexMeta fromTuple(List<?> tuple) {
        Map<String, Object> optionsMap = (Map<String, Object>) tuple.get(VINDEX_OPTIONS_FIELD_NUMBER);

        List<IndexPart> parts = Collections.emptyList();
        List<?> partsTuple = (List<?>) tuple.get(VINDEX_PARTS_FIELD_NUMBER);
        if (!partsTuple.isEmpty()) {
            // simplified index parts as an array
            // (when the parts don't use collation and is_nullable options)
            if (partsTuple.get(0) instanceof List) {
                parts = ((List<List<?>>) partsTuple)
                    .stream()
                    .map(part -> new IndexPart(
                            (Integer) part.get(VINDEX_PART_FIELD),
                            (String) part.get(VINDEX_PART_TYPE)
                        )
                    )
                    .collect(Collectors.toList());
            } else if (partsTuple.get(0) instanceof Map) {
                parts = ((List<Map<String, Object>>) partsTuple)
                    .stream()
                    .map(part -> new IndexPart((Integer) part.get("field"), (String) part.get("type")))
                    .collect(Collectors.toList());
            }
        }

        return new TarantoolIndexMeta(
            (Integer) tuple.get(VINDEX_IID_FIELD_NUMBER),
            (String) tuple.get(VINDEX_NAME_FIELD_NUMBER),
            (String) tuple.get(VINDEX_TYPE_FIELD_NUMBER),
            new IndexOptions((Boolean) optionsMap.get("unique")),
            parts
        );
    }

    public int getId() {
        return id;
    }

    public String getName() {
        return name;
    }

    public String getType() {
        return type;
    }

    public IndexOptions getOptions() {
        return options;
    }

    public List<IndexPart> getParts() {
        return parts;
    }

    public static class IndexOptions {

        private final boolean unique;

        public IndexOptions(boolean unique) {
            this.unique = unique;
        }

        public boolean isUnique() {
            return unique;
        }

    }

    public static class IndexPart {

        private final int fieldNumber;
        private final String type;

        public IndexPart(int fieldNumber, String type) {
            this.fieldNumber = fieldNumber;
            this.type = type;
        }

        public int getFieldNumber() {
            return fieldNumber;
        }

        public String getType() {
            return type;
        }

    }

}
