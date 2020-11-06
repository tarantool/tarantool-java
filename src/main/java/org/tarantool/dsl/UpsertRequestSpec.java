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

public class UpsertRequestSpec extends SpaceRequestSpec<UpsertRequestSpec> {

    private List<Object> key;
    private List<Object> tuple;
    private List<Operation> operations;

    public UpsertRequestSpec(int spaceId, List<?> key, List<?> tuple, Operation... operations) {
        super(Code.UPSERT, spaceId);
        this.key = new ArrayList<>(key);
        this.tuple = new ArrayList<>(tuple);
        this.operations = Arrays.asList(operations);
    }

    public UpsertRequestSpec(String spaceName, List<?> key, List<?> tuple, Operation... operations) {
        super(Code.UPSERT, spaceName);
        this.key = new ArrayList<>(key);
        this.tuple = new ArrayList<>(tuple);
        this.operations = Arrays.asList(operations);
    }

    public UpsertRequestSpec primaryKey(Object... keyParts) {
        this.key.clear();
        Collections.addAll(this.key, keyParts);
        return this;
    }

    public UpsertRequestSpec primaryKey(Collection<?> key) {
        this.key.clear();
        this.key.addAll(key);
        return this;
    }

    public UpsertRequestSpec tuple(Collection<?> tuple) {
        this.tuple.clear();
        this.tuple.addAll(tuple);
        return this;
    }

    public UpsertRequestSpec tuple(Object... tupleItems) {
        this.tuple.clear();
        Collections.addAll(this.tuple, tupleItems);
        return this;
    }

    public UpsertRequestSpec operations(Collection<? extends Operation> operations) {
        this.operations.clear();
        this.operations.addAll(operations);
        return this;
    }

    public UpsertRequestSpec operations(Operation... operations) {
        this.operations.clear();
        Collections.addAll(this.operations, operations);
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
            value(Key.TUPLE), value(tuple),
            value(Key.UPSERT_OPS), value(operations.stream().map(Operation::toArray).collect(Collectors.toList()))
        );
        return request;
    }
}
