package org.tarantool.dsl;

import static org.tarantool.TarantoolRequestArgumentFactory.cacheLookupValue;
import static org.tarantool.TarantoolRequestArgumentFactory.value;

import org.tarantool.Code;
import org.tarantool.Key;
import org.tarantool.TarantoolRequest;
import org.tarantool.schema.TarantoolSchemaMeta;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

public class DeleteRequestSpec extends SpaceRequestSpec<DeleteRequestSpec> {

    private List<Object> key;

    DeleteRequestSpec(int spaceId, List<?> key) {
        super(Code.DELETE, spaceId);
        this.key = new ArrayList<>(key);
    }

    DeleteRequestSpec(int spaceId, Object... keyParts) {
        super(Code.DELETE, spaceId);
        this.key = Arrays.asList(keyParts);
    }

    DeleteRequestSpec(String spaceName, List<?> key) {
        super(Code.DELETE, spaceName);
        this.key = new ArrayList<>(key);
    }

    DeleteRequestSpec(String spaceName, Object... keyParts) {
        super(Code.DELETE, spaceName);
        this.key = Arrays.asList(keyParts);
    }

    public DeleteRequestSpec primaryKey(Object... keyParts) {
        this.key.clear();
        Collections.addAll(this.key, keyParts);
        return this;
    }

    public DeleteRequestSpec primaryKey(Collection<?> key) {
        this.key.clear();
        this.key.addAll(key);
        return this;
    }

    @Override
    public TarantoolRequest toTarantoolRequest(TarantoolSchemaMeta schemaMeta) {
        TarantoolRequest request = super.toTarantoolRequest(schemaMeta);
        request.addArguments(
            value(Key.SPACE),
            spaceId == null
                ? cacheLookupValue(() -> schemaMeta.getSpace(spaceName).getId())
                : value(spaceId),
            value(Key.KEY), value(key)
        );
        return request;
    }

}
