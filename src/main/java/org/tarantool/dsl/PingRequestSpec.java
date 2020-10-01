package org.tarantool.dsl;

import org.tarantool.Code;

public class PingRequestSpec extends AbstractRequestSpec<PingRequestSpec> {

    PingRequestSpec() {
        super(Code.PING);
    }

}
