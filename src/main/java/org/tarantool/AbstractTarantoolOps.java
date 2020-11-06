package org.tarantool;

import static org.tarantool.dsl.Requests.callRequest;
import static org.tarantool.dsl.Requests.deleteRequest;
import static org.tarantool.dsl.Requests.evalRequest;
import static org.tarantool.dsl.Requests.insertRequest;
import static org.tarantool.dsl.Requests.pingRequest;
import static org.tarantool.dsl.Requests.replaceRequest;
import static org.tarantool.dsl.Requests.selectRequest;
import static org.tarantool.dsl.Requests.updateRequest;
import static org.tarantool.dsl.Requests.upsertRequest;

import org.tarantool.dsl.Operation;
import org.tarantool.dsl.TarantoolRequestSpec;
import org.tarantool.logging.Logger;
import org.tarantool.logging.LoggerFactory;
import org.tarantool.schema.TarantoolSchemaMeta;

import java.util.Arrays;
import java.util.List;

public abstract class AbstractTarantoolOps<Result>
    implements TarantoolClientOps<Integer, List<?>, Object, Result> {

    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractTarantoolOps.class);

    private Code callCode = Code.CALL;

    protected abstract Result exec(TarantoolRequest request);

    protected abstract TarantoolSchemaMeta getSchemaMeta();

    public Result select(Integer space, Integer index, List<?> key, int offset, int limit, Iterator iterator) {
        return execute(
            selectRequest(space, index)
                .key(key)
                .offset(offset).limit(limit)
                .iterator(iterator)
        );
    }

    @Override
    public Result select(String space, String index, List<?> key, int offset, int limit, Iterator iterator) {
        return execute(
            selectRequest(space, index)
                .key(key)
                .offset(offset).limit(limit)
                .iterator(iterator)
        );
    }

    @Override
    public Result select(Integer space, Integer index, List<?> key, int offset, int limit, int iterator) {
        return execute(
            selectRequest(space, index)
                .key(key)
                .offset(offset).limit(limit)
                .iterator(iterator)
        );
    }

    @Override
    public Result select(String space, String index, List<?> key, int offset, int limit, int iterator) {
        return execute(
            selectRequest(space, index)
                .key(key)
                .offset(offset).limit(limit)
                .iterator(iterator)
        );
    }

    @Override
    public Result insert(Integer space, List<?> tuple) {
        return execute(insertRequest(space, tuple));
    }

    @Override
    public Result insert(String space, List<?> tuple) {
        return execute(insertRequest(space, tuple));
    }

    @Override
    public Result replace(Integer space, List<?> tuple) {
        return execute(replaceRequest(space, tuple));
    }

    @Override
    public Result replace(String space, List<?> tuple) {
        return execute(replaceRequest(space, tuple));
    }

    @Override
    public Result update(Integer space, List<?> key, Object... operations) {
        Operation[] ops = Arrays.stream(operations)
            .map(Operation::fromArray)
            .toArray(org.tarantool.dsl.Operation[]::new);
        return execute(updateRequest(space, key, ops));
    }

    @Override
    public Result update(String space, List<?> key, Object... operations) {
        Operation[] ops = Arrays.stream(operations)
            .map(Operation::fromArray)
            .toArray(org.tarantool.dsl.Operation[]::new);
        return execute(updateRequest(space, key, ops));
    }

    @Override
    public Result upsert(Integer space, List<?> key, List<?> defTuple, Object... operations) {
        Operation[] ops = Arrays.stream(operations)
            .map(Operation::fromArray)
            .toArray(Operation[]::new);
        return execute(upsertRequest(space, key, defTuple, ops));
    }

    @Override
    public Result upsert(String space, List<?> key, List<?> defTuple, Object... operations) {
        Operation[] ops = Arrays.stream(operations)
            .map(Operation::fromArray)
            .toArray(Operation[]::new);
        return execute(upsertRequest(space, key, defTuple, ops));
    }

    @Override
    public Result delete(Integer space, List<?> key) {
        return execute(deleteRequest(space, key));
    }

    @Override
    public Result delete(String space, List<?> key) {
        return execute(deleteRequest(space, key));
    }

    @Override
    public Result call(String function, Object... args) {
        return execute(
            callRequest(function)
                .arguments(args)
                .useCall16(callCode == Code.OLD_CALL)
        );
    }

    @Override
    public Result eval(String expression, Object... args) {
        return execute(evalRequest(expression).arguments(args));
    }

    @Override
    public void ping() {
        execute(pingRequest());
    }

    @Override
    public Result execute(TarantoolRequestSpec requestSpec) {
        TarantoolSchemaMeta schemaMeta = null;
        try {
            schemaMeta = getSchemaMeta();
        } catch (Exception cause) {
            LOGGER.warn(() -> "Could not get Tarantool schema meta-info", cause);
        }
        return exec(requestSpec.toTarantoolRequest(schemaMeta));
    }

    public void setCallCode(Code callCode) {
        this.callCode = callCode;
    }

}
