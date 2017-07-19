package org.tarantool;


public abstract class AbstractTarantoolOps<Space, Tuple, Operation, Result> implements TarantoolClientOps<Space, Tuple, Operation, Result> {
    private Code callCode = Code.OLD_CALL;

    protected abstract Result exec(Code code, Object... args);

    public Result select(Space space, Space index, Tuple key, int offset, int limit, Iterator iterator) {
        return select(space, index, key, offset, limit, iterator.getValue());
    }

    public Result select(Space space, Space index, Tuple key, int offset, int limit, int iterator) {
        return exec(Code.SELECT, Key.SPACE, space, Key.INDEX, index, Key.KEY, key, Key.ITERATOR, iterator, Key.LIMIT, limit, Key.OFFSET, offset);
    }

    public Result insert(Space space, Tuple tuple) {
        return exec(Code.INSERT, Key.SPACE, space, Key.TUPLE, tuple);
    }

    public Result replace(Space space, Tuple tuple) {
        return exec(Code.REPLACE, Key.SPACE, space, Key.TUPLE, tuple);
    }

    public Result update(Space space, Tuple key, Operation... args) {
        return exec(Code.UPDATE, Key.SPACE, space, Key.KEY, key, Key.TUPLE, args);
    }

    public Result upsert(Space space, Tuple key, Tuple def, Operation... args) {
        return exec(Code.UPSERT, Key.SPACE, space, Key.KEY, key, Key.TUPLE, def, Key.UPSERT_OPS, args);
    }

    public Result delete(Space space, Tuple key) {
        return exec(Code.DELETE, Key.SPACE, space, Key.KEY, key);
    }

    public Result call(String function, Object... args) {
        return exec(callCode, Key.FUNCTION, function, Key.TUPLE, args);
    }

    public Result eval(String expression, Object... args) {
        return exec(Code.EVAL, Key.EXPRESSION, expression, Key.TUPLE, args);
    }

    public void ping() {
        exec(Code.PING);
    }

    public void setCallCode(Code callCode) {
        this.callCode = callCode;
    }
}
