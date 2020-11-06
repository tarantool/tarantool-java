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

public class InsertOrReplaceRequestSpec extends SpaceRequestSpec<InsertOrReplaceRequestSpec> {

    public enum Mode {
        INSERT(Code.INSERT),
        REPLACE(Code.REPLACE);

        final Code code;

        Mode(Code code) {
            this.code = code;
        }

        public Code getCode() {
            return code;
        }
    }

    private List<Object> tuple;

    InsertOrReplaceRequestSpec(Mode mode, int spaceId, List<?> tuple) {
        super(mode.getCode(), spaceId);
        this.tuple = new ArrayList<>(tuple);
    }

    InsertOrReplaceRequestSpec(Mode mode, String spaceName, List<?> tuple) {
        super(mode.getCode(), spaceName);
        this.tuple = new ArrayList<>(tuple);
    }

    InsertOrReplaceRequestSpec(Mode mode, int spaceId, Object... tupleItems) {
        super(mode.getCode(), spaceId);
        this.tuple = Arrays.asList(tupleItems);
    }

    InsertOrReplaceRequestSpec(Mode mode, String spaceName, Object... tupleItems) {
        super(mode.getCode(), spaceName);
        this.tuple = Arrays.asList(tupleItems);
    }

    public InsertOrReplaceRequestSpec tuple(Object... tupleItems) {
        this.tuple.clear();
        Collections.addAll(this.tuple, tupleItems);
        return this;
    }

    public InsertOrReplaceRequestSpec tuple(Collection<?> tuple) {
        this.tuple.clear();
        this.tuple.addAll(tuple);
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
            value(Key.TUPLE), value(tuple)
        );
        return request;
    }

}
