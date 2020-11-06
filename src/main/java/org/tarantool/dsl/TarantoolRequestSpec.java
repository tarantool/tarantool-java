package org.tarantool.dsl;

import org.tarantool.TarantoolRequest;
import org.tarantool.schema.TarantoolSchemaMeta;

/**
 * Used to convert DSL builder to appropriate
 * Tarantool requests.
 *
 * This interface is not a part of public API.
 */
public interface TarantoolRequestSpec {

    /**
     * Converts the target to {@link TarantoolRequest}.
     *
     * @return converted request
     */
    TarantoolRequest toTarantoolRequest(TarantoolSchemaMeta schemaMeta);

}
