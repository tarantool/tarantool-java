package org.tarantool.jdbc;

import org.tarantool.util.ServerVersion;

import java.sql.DatabaseMetaData;
import java.sql.SQLException;

/**
 * Tarantool specific database meta data extension.
 */
public interface TarantoolDatabaseMetaData extends DatabaseMetaData {

    /**
     * Gets the current Tarantool version.
     *
     * @return version of active connected database.
     */
    ServerVersion getDatabaseVersion() throws SQLException;

}
