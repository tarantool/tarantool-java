package org.tarantool.jdbc;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * Tarantool specific statement extensions.
 */
public interface TarantoolStatement extends Statement {

    /**
     * Checks for statement completion and closes itself,
     * according to {@link Statement#closeOnCompletion()}.
     *
     * @throws SQLException if this method is called on a closed
     *                      {@code Statement}
     */
    void checkCompletion() throws SQLException;

    /**
     * Returns {@link ResultSet} which will be initialized by <code>data</code>.
     *
     * @param data predefined result to be wrapped by {@link ResultSet}
     *
     * @return wrapped result
     *
     * @throws SQLException if a database access error occurs or
     *                      this method is called on a closed <code>Statement</code>
     */
    ResultSet executeMetadata(SQLResultHolder data) throws SQLException;

}
