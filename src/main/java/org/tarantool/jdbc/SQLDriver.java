package org.tarantool.jdbc;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.URI;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Logger;

import org.tarantool.TarantoolConnection;

@SuppressWarnings("Since15")
public class SQLDriver implements Driver {

    static {
        try {
            java.sql.DriverManager.registerDriver(new SQLDriver());
        } catch (SQLException E) {
            throw new RuntimeException("Can't register driver!");
        }
    }

    public static final String PROP_HOST = "host";
    public static final String PROP_PORT = "port";
    public static final String PROP_SOCKET_PROVIDER = "socketProvider";
    public static final String PROP_USER = "user";
    public static final String PROP_PASSWORD = "password";


    protected Map<String, SQLSocketProvider> providerCache = new ConcurrentHashMap<String, SQLSocketProvider>();

    @Override
    public Connection connect(String url, Properties info) throws SQLException {
        URI uri = URI.create(url);
        Properties urlProperties = parseQueryString(uri, info);
        String providerClassName = urlProperties.getProperty(PROP_SOCKET_PROVIDER);
        Socket socket;
        if (providerClassName != null) {
            socket = getSocketFromProvider(uri, urlProperties, providerClassName);
        } else {
            socket = createAndConnectDefaultSocket(urlProperties);
        }
        try {
            TarantoolConnection connection = new TarantoolConnection(urlProperties.getProperty(PROP_USER), urlProperties.getProperty(PROP_PASSWORD), socket) {{
                msgPackLite = SQLMsgPackLite.INSTANCE;
            }};

            return new SQLConnection(connection, url, info);
        } catch (IOException e) {
            throw new SQLException("Couldn't initiate connection. Provider class name is " + providerClassName, e);
        }

    }

    protected Properties parseQueryString(URI uri, Properties info) {
        Properties urlProperties = new Properties(info);
        if (uri.getQuery() != null) {
            String[] parts = uri.getQuery().split("&");
            for (String part : parts) {
                int i = part.indexOf("=");
                if (i > -1) {
                    urlProperties.put(part.substring(0, i), part.substring(i + 1));
                } else {
                    urlProperties.put(part, "");
                }
            }
        }
        urlProperties.put(PROP_HOST, uri.getHost() == null ? "localhost" : uri.getHost());
        urlProperties.put(PROP_PORT, uri.getPort() < 1 ? "3301" : uri.getPort());
        return urlProperties;
    }

    protected Socket createAndConnectDefaultSocket(Properties properties) throws SQLException {
        Socket socket;
        socket = new Socket();
        try {
            socket.connect(new InetSocketAddress(properties.getProperty(PROP_HOST, "localhost"), Integer.parseInt(properties.getProperty(PROP_PORT, "3301"))));
        } catch (Exception e) {
            throw new SQLException("Couldn't connect to tarantool using" + properties, e);
        }
        return socket;
    }

    protected Socket getSocketFromProvider(URI uri, Properties urlProperties, String providerClassName)
            throws SQLException {
        Socket socket;
        SQLSocketProvider sqlSocketProvider = providerCache.get(providerClassName);
        if (sqlSocketProvider == null) {
            synchronized (this) {
                sqlSocketProvider = providerCache.get(providerClassName);
                if (sqlSocketProvider == null) {
                    try {
                        Class<?> cls = Class.forName(providerClassName);
                        if (SQLSocketProvider.class.isAssignableFrom(cls)) {
                            sqlSocketProvider = (SQLSocketProvider) cls.newInstance();
                            providerCache.put(providerClassName, sqlSocketProvider);
                        }
                    } catch (Exception e) {
                        throw new SQLException("Couldn't initiate socket provider " + providerClassName, e);
                    }
                }
            }
        }
        socket = sqlSocketProvider.getConnectedSocket(uri, urlProperties);
        return socket;
    }

    @Override
    public boolean acceptsURL(String url) throws SQLException {
        return url.toLowerCase().startsWith("tarantool:");
    }

    @Override
    public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) throws SQLException {
        try {
            URI uri = new URI(url);
            Properties properties = parseQueryString(uri, new Properties(info == null ? new Properties() : info));

            DriverPropertyInfo host = new DriverPropertyInfo(PROP_HOST, properties.getProperty(PROP_HOST));
            host.required = true;
            host.description = "Tarantool sever host";

            DriverPropertyInfo port = new DriverPropertyInfo(PROP_PORT, properties.getProperty(PROP_PORT));
            port.required = true;
            port.description = "Tarantool sever port";


            DriverPropertyInfo user = new DriverPropertyInfo(PROP_USER, properties.getProperty(PROP_USER));
            user.required = false;
            user.description = "user";

            DriverPropertyInfo password = new DriverPropertyInfo(PROP_PASSWORD, properties.getProperty(PROP_PASSWORD));
            password.required = false;
            password.description = "password";

            DriverPropertyInfo socketProvider = new DriverPropertyInfo(PROP_SOCKET_PROVIDER, properties.getProperty(PROP_SOCKET_PROVIDER));
            socketProvider.required = false;
            socketProvider.description = "SocketProvider class implements org.tarantool.jdbc.SQLSocketProvider";


            return new DriverPropertyInfo[]{host, port, user, password, socketProvider};
        } catch (Exception e) {
            throw new SQLException(e);
        }
    }

    @Override
    public int getMajorVersion() {
        return 0;
    }

    @Override
    public int getMinorVersion() {
        return 1;
    }

    @Override
    public boolean jdbcCompliant() {
        return false;
    }

    @Override
    public Logger getParentLogger() throws SQLFeatureNotSupportedException {
        throw new SQLFeatureNotSupportedException();
    }


}
