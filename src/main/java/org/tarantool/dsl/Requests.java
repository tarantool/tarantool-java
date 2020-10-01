package org.tarantool.dsl;

import org.tarantool.dsl.InsertOrReplaceRequestSpec.Mode;

import java.util.List;

/**
 * Entry point to build requests
 * using DSL approach a.k.a Request DSL.
 */
public class Requests {

    public static SelectRequestSpec selectRequest(int space, int index) {
        return new SelectRequestSpec(space, index);
    }

    public static SelectRequestSpec selectRequest(String space, int index) {
        return new SelectRequestSpec(space, index);
    }

    public static SelectRequestSpec selectRequest(int space, String index) {
        return new SelectRequestSpec(space, index);
    }

    public static SelectRequestSpec selectRequest(String space, String index) {
        return new SelectRequestSpec(space, index);
    }

    public static InsertOrReplaceRequestSpec insertRequest(int space, List<?> tuple) {
        return new InsertOrReplaceRequestSpec(Mode.INSERT, space, tuple);
    }

    public static InsertOrReplaceRequestSpec insertRequest(int space, Object... tupleItems) {
        return new InsertOrReplaceRequestSpec(Mode.INSERT, space, tupleItems);
    }

    public static InsertOrReplaceRequestSpec insertRequest(String space, List<?> tuple) {
        return new InsertOrReplaceRequestSpec(Mode.INSERT, space, tuple);
    }

    public static InsertOrReplaceRequestSpec insertRequest(String space, Object... tupleItems) {
        return new InsertOrReplaceRequestSpec(Mode.INSERT, space, tupleItems);
    }

    public static InsertOrReplaceRequestSpec replaceRequest(int space, List<?> tuple) {
        return new InsertOrReplaceRequestSpec(Mode.REPLACE, space, tuple);
    }

    public static InsertOrReplaceRequestSpec replaceRequest(int space, Object... tupleItems) {
        return new InsertOrReplaceRequestSpec(Mode.REPLACE, space, tupleItems);
    }

    public static InsertOrReplaceRequestSpec replaceRequest(String space, List<?> tuple) {
        return new InsertOrReplaceRequestSpec(Mode.REPLACE, space, tuple);
    }

    public static InsertOrReplaceRequestSpec replaceRequest(String space, Object... tupleItems) {
        return new InsertOrReplaceRequestSpec(Mode.REPLACE, space, tupleItems);
    }

    public static UpdateRequestSpec updateRequest(int space, List<?> key, Operation... operations) {
        return new UpdateRequestSpec(space, key, operations);
    }

    public static UpdateRequestSpec updateRequest(String space, List<?> key, Operation... operations) {
        return new UpdateRequestSpec(space, key, operations);
    }

    public static UpsertRequestSpec upsertRequest(int space, List<?> key, List<?> tuple, Operation... operations) {
        return new UpsertRequestSpec(space, key, tuple, operations);
    }

    public static UpsertRequestSpec upsertRequest(String space, List<?> key, List<?> tuple, Operation... operations) {
        return new UpsertRequestSpec(space, key, tuple, operations);
    }

    public static DeleteRequestSpec deleteRequest(int space, List<?> key) {
        return new DeleteRequestSpec(space, key);
    }

    public static DeleteRequestSpec deleteRequest(int space, Object... keyParts) {
        return new DeleteRequestSpec(space, keyParts);
    }

    public static DeleteRequestSpec deleteRequest(String space, List<?> key) {
        return new DeleteRequestSpec(space, key);
    }

    public static DeleteRequestSpec deleteRequest(String space, Object... keyParts) {
        return new DeleteRequestSpec(space, keyParts);
    }

    public static CallRequestSpec callRequest(String function) {
        return new CallRequestSpec(function);
    }

    public static EvalRequestSpec evalRequest(String expression) {
        return new EvalRequestSpec(expression);
    }

    public static PingRequestSpec pingRequest() {
        return new PingRequestSpec();
    }

    public static ExecuteRequestSpec executeRequest(String sql) {
        return new ExecuteRequestSpec(sql);
    }

}
