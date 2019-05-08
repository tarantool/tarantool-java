package org.tarantool.jdbc.ds;

import org.tarantool.jdbc.SQLConnection;
import org.tarantool.jdbc.SQLConstant;
import org.tarantool.jdbc.SQLProperty;

import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLNonTransientException;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;
import javax.sql.DataSource;

/**
 * Simple {@code java.sql.DataSource} implementation.
 */
public class SQLDataSource implements TarantoolDataSource, DataSource {

    private PrintWriter logWriter;
    private String name = "Tarantool basic data source";

    private Properties properties = new Properties();

    @Override
    public Connection getConnection() throws SQLException {
        return new SQLConnection(makeUrl(), new Properties(properties));
    }

    @Override
    public Connection getConnection(String username, String password) throws SQLException {
        Properties copyProperties = new Properties(properties);
        SQLProperty.USER.setString(copyProperties, username);
        SQLProperty.PASSWORD.setString(copyProperties, password);
        return new SQLConnection(makeUrl(), copyProperties);
    }

    @Override
    public PrintWriter getLogWriter() {
        return logWriter;
    }

    @Override
    public void setLogWriter(PrintWriter out) {
        logWriter = out;
    }

    @Override
    public void setLoginTimeout(int seconds) {
        SQLProperty.LOGIN_TIMEOUT.setInt(properties, (int) TimeUnit.SECONDS.toMillis(seconds));
    }

    @Override
    public int getLoginTimeout() throws SQLException {
        return (int) TimeUnit.MILLISECONDS.toSeconds(SQLProperty.LOGIN_TIMEOUT.getInt(properties));
    }

    @Override
    public Logger getParentLogger() throws SQLFeatureNotSupportedException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public <T> T unwrap(Class<T> type) throws SQLException {
        if (isWrapperFor(type)) {
            return type.cast(this);
        }
        throw new SQLNonTransientException("SQLDataSource does not wrap " + type.getName());
    }

    @Override
    public boolean isWrapperFor(Class<?> type) {
        return type.isAssignableFrom(this.getClass());
    }

    @Override
    public String getServerName() {
        return SQLProperty.HOST.getString(properties);
    }

    @Override
    public void setServerName(String serverName) {
        SQLProperty.HOST.setString(properties, serverName);
    }

    @Override
    public int getPortNumber() throws SQLException {
        return SQLProperty.PORT.getInt(properties);
    }

    @Override
    public void setPortNumber(int port) {
        SQLProperty.PORT.setInt(properties, port);
    }

    @Override
    public String getUser() {
        return SQLProperty.USER.getString(properties);
    }

    @Override
    public void setUser(String userName) {
        SQLProperty.USER.setString(properties, userName);
    }

    @Override
    public String getPassword() {
        return SQLProperty.PASSWORD.getString(properties);
    }

    @Override
    public void setPassword(String password) {
        SQLProperty.PASSWORD.setString(properties, password);
    }

    @Override
    public String getDescription() {
        return "Basic DataSource implementation - produces a standard Connection object. " +
            SQLConstant.DRIVER_NAME + ".";
    }

    @Override
    public String getDataSourceName() {
        return name;
    }

    @Override
    public void setDataSourceName(String name) {
        this.name = name;
    }

    @Override
    public String getSocketChannelProvider() {
        return SQLProperty.SOCKET_CHANNEL_PROVIDER.getString(properties);
    }

    @Override
    public void setSocketChannelProvider(String classFqdn) {
        SQLProperty.SOCKET_CHANNEL_PROVIDER.setString(properties, classFqdn);
    }

    @Override
    public int getQueryTimeout() throws SQLException {
        return (int) TimeUnit.MILLISECONDS.toSeconds(SQLProperty.QUERY_TIMEOUT.getInt(properties));
    }

    @Override
    public void setQueryTimeout(int seconds) {
        SQLProperty.QUERY_TIMEOUT.setInt(properties, (int) TimeUnit.SECONDS.toMillis(seconds));
    }

    private String makeUrl() {
        return "jdbc:tarantool://" +
            SQLProperty.HOST.getString(properties) + ":" + SQLProperty.PORT.getString(properties);
    }

}
