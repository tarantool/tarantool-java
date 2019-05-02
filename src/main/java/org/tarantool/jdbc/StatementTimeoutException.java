package org.tarantool.jdbc;

import java.sql.SQLTimeoutException;

public class StatementTimeoutException extends SQLTimeoutException {

    public StatementTimeoutException(String reason, Throwable cause) {
        super(reason, cause);
    }

}
