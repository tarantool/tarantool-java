package org.tarantool.jdbc;

import java.sql.SQLTimeoutException;

public class StatementTimeoutException extends SQLTimeoutException {
    private static final long serialVersionUID = -1L;

    public StatementTimeoutException(String reason, Throwable cause) {
        super(reason, cause);
    }

}
