package org.tarantool.dsl;

import static org.tarantool.TarantoolRequestArgumentFactory.value;

import org.tarantool.Code;
import org.tarantool.Key;
import org.tarantool.TarantoolRequest;
import org.tarantool.schema.TarantoolSchemaMeta;
import org.tarantool.util.TupleTwo;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

public class ExecuteRequestSpec extends AbstractRequestSpec<ExecuteRequestSpec> {

    private String sqlText;
    private List<Object> ordinalBindings = new ArrayList<>();
    private List<TupleTwo<String, Object>> namedBindings = new ArrayList<>();

    ExecuteRequestSpec(String sqlText) {
        super(Code.EXECUTE);
        this.sqlText = Objects.requireNonNull(sqlText);
    }

    public ExecuteRequestSpec sql(String text) {
        Objects.requireNonNull(text);
        this.sqlText = text;
        return this;
    }

    public ExecuteRequestSpec ordinalParameters(Object... bindings) {
        this.ordinalBindings.clear();
        Collections.addAll(this.ordinalBindings, bindings);
        this.namedBindings.clear();
        return this;
    }

    public ExecuteRequestSpec ordinalParameters(Collection<?> bindings) {
        this.ordinalBindings.clear();
        this.ordinalBindings.addAll(bindings);
        this.namedBindings.clear();
        return this;
    }

    public ExecuteRequestSpec namedParameters(Map<String, ?> bindings) {
        this.namedBindings.clear();
        this.namedBindings.addAll(
            bindings.entrySet().stream()
                .map(e -> TupleTwo.<String, Object>of(e.getKey(), e.getValue()))
                .collect(Collectors.toList())
        );
        this.ordinalBindings.clear();
        return this;
    }

    public ExecuteRequestSpec namedParameters(TupleTwo<String, ?>[] bindings) {
        this.namedBindings.clear();
        for (TupleTwo<String, ?> binding : bindings) {
            this.namedBindings.add(TupleTwo.of(binding.getFirst(), binding.getSecond()));
        }
        this.ordinalBindings.clear();
        return this;
    }

    @Override
    public TarantoolRequest toTarantoolRequest(TarantoolSchemaMeta schemaMeta) {
        TarantoolRequest request = super.toTarantoolRequest(schemaMeta);
        request.addArguments(
            value(Key.SQL_TEXT),
            value(sqlText)
        );
        if (!ordinalBindings.isEmpty()) {
            request.addArguments(
                value(Key.SQL_BIND),
                value(ordinalBindings)
            );
        }
        if (!namedBindings.isEmpty()) {
            request.addArguments(
                value(Key.SQL_BIND),
                value(namedBindings)
            );
        }
        return request;
    }

}
