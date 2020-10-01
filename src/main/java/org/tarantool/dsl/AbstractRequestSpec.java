package org.tarantool.dsl;

import org.tarantool.Code;
import org.tarantool.TarantoolRequest;
import org.tarantool.schema.TarantoolSchemaMeta;

import java.time.Duration;

public abstract class AbstractRequestSpec<B extends AbstractRequestSpec<B>>
    implements TarantoolRequestSpec {

    final Code code;
    Duration duration = Duration.ZERO;
    boolean useDefaultTimeout = true;

    AbstractRequestSpec(Code code) {
        this.code = code;
    }

    AbstractRequestSpec(Code code, Duration duration) {
        this.code = code;
        this.duration = duration;
    }

    @SuppressWarnings("unchecked")
    public B timeout(Duration duration) {
        this.duration = duration;
        this.useDefaultTimeout = false;
        return (B) this;
    }

    @SuppressWarnings("unchecked")
    public B useDefaultTimeout() {
        this.duration = Duration.ZERO;
        this.useDefaultTimeout = true;
        return (B) this;
    }

    @Override
    public TarantoolRequest toTarantoolRequest(TarantoolSchemaMeta schemaMeta) {
        TarantoolRequest request = new TarantoolRequest(code);
        request.setTimeout(useDefaultTimeout ? null : duration);
        return request;
    }

}
