package org.tarantool.schema;

public class TarantoolSpaceNotFoundException extends TarantoolSchemaException {

    public TarantoolSpaceNotFoundException(String spaceName) {
        super(spaceName);
    }

}
