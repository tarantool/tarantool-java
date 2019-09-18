package org.tarantool.schema;

public class TarantoolIndexNotFoundException extends TarantoolSchemaException {

    private final String indexName;

    public TarantoolIndexNotFoundException(String targetSpace, String indexName) {
        super(targetSpace);
        this.indexName = indexName;
    }

    public String getIndexName() {
        return indexName;
    }

}
