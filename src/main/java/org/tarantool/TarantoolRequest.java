package org.tarantool;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Describes a static request parameters.
 */
public class TarantoolRequest {

    /**
     * Tarantool binary protocol operation code.
     */
    private Code code;

    /**
     * Arguments of operation.
     */
    private List<TarantoolRequestArgument> arguments;

    /**
     * Request timeout start just after initialization.
     */
    private Duration timeout;

    public TarantoolRequest(Code code) {
        this.code = code;
        this.arguments = new ArrayList<>();
    }

    public TarantoolRequest(Code code, TarantoolRequestArgument... arguments) {
        this.code = code;
        this.arguments = Arrays.asList(arguments);
    }

    /**
     * Initializes an operation and starts its timer.
     *
     * @param sid      internal request id
     * @param schemaId schema version
     */
    TarantoolOperation toOperation(long sid, long schemaId) {
        return new TarantoolOperation(code, arguments, sid, schemaId, timeout);
    }

    /**
     * Initializes a preflight operation that
     * will be processed before the dependent.
     *
     * @param sid       internal request id
     * @param schemaId  schema version
     * @param operation depended operation
     */
    TarantoolOperation toPreflightOperation(long sid, long schemaId, TarantoolOperation operation) {
        return new TarantoolOperation(code, arguments, sid, schemaId, timeout, operation);
    }


    public Code getCode() {
        return code;
    }

    public void setCode(Code code) {
        this.code = code;
    }

    public Duration getTimeout() {
        return timeout;
    }

    public void setTimeout(Duration timeout) {
        this.timeout = timeout;
    }

    public List<Object> getArguments() {
        return arguments.stream().map(TarantoolRequestArgument::getValue).collect(Collectors.toList());
    }

    public void addArguments(TarantoolRequestArgument... arguments) {
        this.arguments.addAll(Arrays.asList(arguments));
    }

}
