package org.tarantool;

import static org.tarantool.TarantoolRequestArgumentFactory.cacheLookupValue;
import static org.tarantool.TarantoolRequestArgumentFactory.value;

import org.tarantool.schema.TarantoolSchemaMeta;

import java.util.List;

public abstract class AbstractTarantoolOps<Result>
    implements TarantoolClientOps<Integer, List<?>, Object, Result> {

    private Code callCode = Code.CALL;

    protected abstract Result exec(TarantoolRequest request);

    protected abstract TarantoolSchemaMeta getSchemaMeta();

    public Result select(Integer space, Integer index, List<?> key, int offset, int limit, Iterator iterator) {
        return select(space, index, key, offset, limit, iterator.getValue());
    }

    @Override
    public Result select(String space, String index, List<?> key, int offset, int limit, Iterator iterator) {
        return select(space, index, key, offset, limit, iterator.getValue());
    }

    @Override
    public Result select(Integer space, Integer index, List<?> key, int offset, int limit, int iterator) {
        return exec(
            new TarantoolRequest(
                Code.SELECT,
                value(Key.SPACE), value(space),
                value(Key.INDEX), value(index),
                value(Key.KEY), value(key),
                value(Key.ITERATOR), value(iterator),
                value(Key.LIMIT), value(limit),
                value(Key.OFFSET), value(offset)
            )
        );
    }

    @Override
    public Result select(String space, String index, List<?> key, int offset, int limit, int iterator) {
        return exec(
            new TarantoolRequest(
                Code.SELECT,
                value(Key.SPACE), cacheLookupValue(() -> getSchemaMeta().getSpace(space).getId()),
                value(Key.INDEX), cacheLookupValue(() -> getSchemaMeta().getSpaceIndex(space, index).getId()),
                value(Key.KEY), value(key),
                value(Key.ITERATOR), value(iterator),
                value(Key.LIMIT), value(limit),
                value(Key.OFFSET), value(offset)
            )
        );
    }

    @Override
    public Result insert(Integer space, List<?> tuple) {
        return exec(new TarantoolRequest(
                Code.INSERT,
                value(Key.SPACE), value(space),
                value(Key.TUPLE), value(tuple)
            )
        );
    }

    @Override
    public Result insert(String space, List<?> tuple) {
        return exec(
            new TarantoolRequest(
                Code.INSERT,
                value(Key.SPACE), cacheLookupValue(() -> getSchemaMeta().getSpace(space).getId()),
                value(Key.TUPLE), value(tuple)
            )
        );
    }

    @Override
    public Result replace(Integer space, List<?> tuple) {
        return exec(
            new TarantoolRequest(
                Code.REPLACE,
                value(Key.SPACE), value(space),
                value(Key.TUPLE), value(tuple)
            )
        );
    }

    @Override
    public Result replace(String space, List<?> tuple) {
        return exec(
            new TarantoolRequest(
                Code.REPLACE,
                value(Key.SPACE), cacheLookupValue(() -> getSchemaMeta().getSpace(space).getId()),
                value(Key.TUPLE), value(tuple)
            )
        );
    }

    @Override
    public Result update(Integer space, List<?> key, Object... operations) {
        return exec(
            new TarantoolRequest(
                Code.UPDATE,
                value(Key.SPACE), value(space),
                value(Key.KEY), value(key),
                value(Key.TUPLE), value(operations)
            )
        );
    }

    @Override
    public Result update(String space, List<?> key, Object... operations) {
        return exec(
            new TarantoolRequest(
                Code.UPDATE,
                value(Key.SPACE), cacheLookupValue(() -> getSchemaMeta().getSpace(space).getId()),
                value(Key.KEY), value(key),
                value(Key.TUPLE), value(operations)
            )
        );
    }

    @Override
    public Result upsert(Integer space, List<?> key, List<?> defTuple, Object... operations) {
        return exec(
            new TarantoolRequest(
                Code.UPSERT,
                value(Key.SPACE), value(space),
                value(Key.KEY), value(key),
                value(Key.TUPLE), value(defTuple),
                value(Key.UPSERT_OPS), value(operations)
            )
        );
    }

    @Override
    public Result upsert(String space, List<?> key, List<?> defTuple, Object... operations) {
        return exec(
            new TarantoolRequest(
                Code.UPSERT,
                value(Key.SPACE), cacheLookupValue(() -> getSchemaMeta().getSpace(space).getId()),
                value(Key.KEY), value(key),
                value(Key.TUPLE), value(defTuple),
                value(Key.UPSERT_OPS), value(operations)
            )
        );
    }

    @Override
    public Result delete(Integer space, List<?> key) {
        return exec(
            new TarantoolRequest(
                Code.DELETE,
                value(Key.SPACE), value(space),
                value(Key.KEY), value(key)
            )
        );
    }

    @Override
    public Result delete(String space, List<?> key) {
        return exec(
            new TarantoolRequest(
                Code.DELETE,
                value(Key.SPACE), cacheLookupValue(() -> getSchemaMeta().getSpace(space).getId()),
                value(Key.KEY), value(key)
            )
        );
    }

    @Override
    public Result call(String function, Object... args) {
        return exec(
            new TarantoolRequest(
                callCode,
                value(Key.FUNCTION), value(function),
                value(Key.TUPLE), value(args)
            )
        );
    }

    @Override
    public Result eval(String expression, Object... args) {
        return exec(
            new TarantoolRequest(
                Code.EVAL,
                value(Key.EXPRESSION), value(expression),
                value(Key.TUPLE), value(args)
            )
        );
    }

    @Override
    public void ping() {
        exec(new TarantoolRequest(Code.PING));
    }

    public void setCallCode(Code callCode) {
        this.callCode = callCode;
    }

}
