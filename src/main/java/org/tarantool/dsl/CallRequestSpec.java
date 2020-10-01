package org.tarantool.dsl;

import static org.tarantool.TarantoolRequestArgumentFactory.value;

import org.tarantool.Code;
import org.tarantool.Key;
import org.tarantool.TarantoolRequest;
import org.tarantool.schema.TarantoolSchemaMeta;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

public class CallRequestSpec extends AbstractRequestSpec<CallRequestSpec> {

    private String functionName;
    private List<Object> arguments = new ArrayList<>();
    private boolean useCall16 = false;

    CallRequestSpec(String functionName) {
        super(Code.CALL);
        this.functionName = Objects.requireNonNull(functionName);
    }

    public CallRequestSpec function(String functionName) {
        Objects.requireNonNull(functionName);
        this.functionName = functionName;
        return this;
    }

    public CallRequestSpec arguments(Object... arguments) {
        this.arguments.clear();
        Collections.addAll(this.arguments, arguments);
        return this;
    }

    public CallRequestSpec arguments(Collection<?> arguments) {
        this.arguments.clear();
        this.arguments.addAll(arguments);
        return this;
    }

    public CallRequestSpec useCall16(boolean flag) {
        this.useCall16 = flag;
        return this;
    }

    @Override
    public TarantoolRequest toTarantoolRequest(TarantoolSchemaMeta schemaMeta) {
        TarantoolRequest request = super.toTarantoolRequest(schemaMeta);
        if (useCall16) {
            request.setCode(Code.OLD_CALL);
        }
        request.addArguments(
            value(Key.FUNCTION), value(functionName),
            value(Key.TUPLE), value(arguments)
        );
        return request;
    }

}
