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
import java.util.stream.Collectors;

public class UpdateRequestSpec extends SpaceRequestSpec<UpdateRequestSpec> {

    private List<Object> key;
    private List<Operation> operations;

    public UpdateRequestSpec(int spaceId, List<?> key, Operation... operations) {
        super(Code.UPDATE, spaceId);
        this.key = new ArrayList<>(key);
        this.operations = Arrays.asList(operations);
    }

    public UpdateRequestSpec(String spaceName, List<?> key, Operation... operations) {
        super(Code.UPDATE, spaceName);
        this.key = new ArrayList<>(key);
        this.operations = Arrays.asList(operations);
    }

    public UpdateRequestSpec primaryKey(Object... keyParts) {
        this.key.clear();
        Collections.addAll(this.key, keyParts);
        return this;
    }

    public UpdateRequestSpec primaryKey(Collection<?> key) {
        this.key.clear();
        this.key.addAll(key);
        return this;
    }

    public UpdateRequestSpec operations(Operation... operations) {
        this.operations.clear();
        Collections.addAll(this.operations, operations);
        return this;
    }

    public UpdateRequestSpec operations(Collection<? extends Operation> operations) {
        this.operations.clear();
        this.operations.addAll(operations);
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
            value(Key.KEY), value(key),
            value(Key.TUPLE), value(operations.stream().map(Operation::toArray).collect(Collectors.toList()))
        );
        return request;
    }

}
