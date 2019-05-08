package org.tarantool.jdbc.ds;

import java.sql.SQLException;

/**
 * JDBC standard  Tarantool specific data source properties.
 */
public interface TarantoolDataSource {

    String getServerName() throws SQLException;

    void setServerName(String serverName) throws SQLException;

    int getPortNumber() throws SQLException;

    void setPortNumber(int port) throws SQLException;

    String getUser() throws SQLException;

    void setUser(String userName) throws SQLException;

    String getPassword() throws SQLException;

    void setPassword(String password) throws SQLException;

    String getDescription() throws SQLException;

    String getDataSourceName() throws SQLException;

    void setDataSourceName(String name) throws SQLException;

    String getSocketChannelProvider() throws SQLException;

    void setSocketChannelProvider(String classFqdn) throws SQLException;

    int getQueryTimeout() throws SQLException;

    void setQueryTimeout(int seconds) throws SQLException;

}
