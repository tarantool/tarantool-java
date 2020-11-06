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

public class EvalRequestSpec extends AbstractRequestSpec<EvalRequestSpec> {

    private String expression;
    private List<Object> arguments = new ArrayList<>();

    EvalRequestSpec(String expression) {
        super(Code.EVAL);
        this.expression = Objects.requireNonNull(expression);
    }

    public EvalRequestSpec expression(String expression) {
        Objects.requireNonNull(expression);
        this.expression = expression;
        return this;
    }

    public EvalRequestSpec arguments(Object... arguments) {
        this.arguments.clear();
        Collections.addAll(this.arguments, arguments);
        return this;
    }

    public EvalRequestSpec arguments(Collection<?> arguments) {
        this.arguments.clear();
        this.arguments.addAll(arguments);
        return this;
    }

    @Override
    public TarantoolRequest toTarantoolRequest(TarantoolSchemaMeta schemaMeta) {
        TarantoolRequest request = super.toTarantoolRequest(schemaMeta);
        request.addArguments(
            value(Key.EXPRESSION), value(expression),
            value(Key.TUPLE), value(arguments)
        );
        return request;
    }

}
