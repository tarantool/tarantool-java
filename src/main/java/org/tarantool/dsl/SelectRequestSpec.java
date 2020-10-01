package org.tarantool.dsl;

import static org.tarantool.TarantoolRequestArgumentFactory.cacheLookupValue;
import static org.tarantool.TarantoolRequestArgumentFactory.value;

import org.tarantool.Code;
import org.tarantool.Iterator;
import org.tarantool.Key;
import org.tarantool.TarantoolRequest;
import org.tarantool.schema.TarantoolSchemaMeta;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

public class SelectRequestSpec extends SpaceRequestSpec<SelectRequestSpec> {

    private Integer indexId;
    private String indexName;
    private List<Object> key = new ArrayList<>();
    private Iterator iterator = Iterator.ALL;
    private int offset = 0;
    private int limit = Integer.MAX_VALUE;

    public SelectRequestSpec(int spaceId, int indexId) {
        super(Code.SELECT, spaceId);
        this.indexId = indexId;
    }

    public SelectRequestSpec(int spaceId, String indexName) {
        super(Code.SELECT, spaceId);
        this.indexName = Objects.requireNonNull(indexName);
    }

    public SelectRequestSpec(String spaceName, int indexId) {
        super(Code.SELECT, spaceName);
        this.indexId = indexId;
    }

    public SelectRequestSpec(String spaceName, String indexName) {
        super(Code.SELECT, spaceName);
        this.indexName = Objects.requireNonNull(indexName);
    }

    public SelectRequestSpec index(int indexId) {
        this.indexId = indexId;
        this.indexName = null;
        return this;
    }

    public SelectRequestSpec index(String indexName) {
        this.indexName = Objects.requireNonNull(indexName);
        this.indexId = null;
        return this;
    }

    public SelectRequestSpec key(Object... keyParts) {
        this.key.clear();
        Collections.addAll(this.key, keyParts);
        return this;
    }

    public SelectRequestSpec key(Collection<?> key) {
        this.key.clear();
        this.key.addAll(key);
        return this;
    }

    public SelectRequestSpec iterator(Iterator iterator) {
        this.iterator = iterator;
        return this;
    }

    public SelectRequestSpec iterator(int iterator) {
        this.iterator = Iterator.valueOf(iterator);
        return this;
    }

    public SelectRequestSpec offset(int offset) {
        this.offset = offset;
        return this;
    }

    public SelectRequestSpec limit(int limit) {
        this.limit = limit;
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
            value(Key.INDEX),
            indexId == null
                ? cacheLookupValue(() -> schemaMeta.getSpaceIndex(spaceName, indexName).getId())
                : value(indexId),
            value(Key.KEY), value(key),
            value(Key.ITERATOR), value(iterator.getValue()),
            value(Key.LIMIT), value(limit),
            value(Key.OFFSET), value(offset)
        );
        return request;
    }

}
