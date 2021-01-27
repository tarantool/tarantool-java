package org.tarantool.schema;

public class TarantoolSpaceNotFoundException extends TarantoolSchemaException {
    private static final long serialVersionUID = -1L;

    public TarantoolSpaceNotFoundException(String spaceName) {
        super(spaceName);
    }

}
