package org.tarantool.schema;

public class TarantoolSchemaException extends RuntimeException {
    private static final long serialVersionUID = -1L;

    private final String schemaName;

    public TarantoolSchemaException(String schemaName) {
        this.schemaName = schemaName;
    }

    public String getSchemaName() {
        return schemaName;
    }

}
