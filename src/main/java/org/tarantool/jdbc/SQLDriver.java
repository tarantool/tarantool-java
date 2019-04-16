package org.tarantool.jdbc;

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

public class SQLDriver implements Driver {

    static {
        try {
            java.sql.DriverManager.registerDriver(new SQLDriver());
        } catch (SQLException e) {
            throw new RuntimeException("Can't register driver!");
        }
    }

    public static final String PROP_HOST = "host";
    public static final String PROP_PORT = "port";
    public static final String PROP_SOCKET_PROVIDER = "socketProvider";
    public static final String PROP_USER = "user";
    public static final String PROP_PASSWORD = "password";
    public static final String PROP_SOCKET_TIMEOUT = "socketTimeout";

    // Define default values once here.
    static final Properties defaults = new Properties() {
        {
            setProperty(PROP_HOST, "localhost");
            setProperty(PROP_PORT, "3301");
            setProperty(PROP_SOCKET_TIMEOUT, "0");
        }
    };

    private final Map<String, SQLSocketProvider> providerCache = new ConcurrentHashMap<String, SQLSocketProvider>();

    @Override
    public Connection connect(String url, Properties info) throws SQLException {
        final URI uri = URI.create(url);
        final Properties urlProperties = parseQueryString(uri, info);
        String providerClassName = urlProperties.getProperty(PROP_SOCKET_PROVIDER);

        if (providerClassName == null) {
            return new SQLConnection(url, urlProperties);
        }

        final SQLSocketProvider provider = getSocketProviderInstance(providerClassName);

        return new SQLConnection(url, urlProperties) {
            @Override
            protected Socket getConnectedSocket() throws SQLException {
                Socket socket = provider.getConnectedSocket(uri, urlProperties);
                if (socket == null) {
                    throw new SQLException("The socket provider returned null socket");
                }
                return socket;
            }
        };
    }

    protected Properties parseQueryString(URI uri, Properties info) throws SQLException {
        Properties urlProperties = new Properties(defaults);

        String userInfo = uri.getUserInfo();
        if (userInfo != null) {
            // Get user and password from the corresponding part of the URI, i.e. before @ sign.
            int i = userInfo.indexOf(':');
            if (i < 0) {
                urlProperties.setProperty(PROP_USER, userInfo);
            } else {
                urlProperties.setProperty(PROP_USER, userInfo.substring(0, i));
                urlProperties.setProperty(PROP_PASSWORD, userInfo.substring(i + 1));
            }
        }
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
        if (uri.getHost() != null) {
            // Default values are pre-put above.
            urlProperties.setProperty(PROP_HOST, uri.getHost());
        }
        if (uri.getPort() >= 0) {
            // We need to convert port to string otherwise getProperty() will not see it.
            urlProperties.setProperty(PROP_PORT, String.valueOf(uri.getPort()));
        }
        if (info != null) {
            urlProperties.putAll(info);
        }

        // Validate properties.
        int port;
        try {
            port = Integer.parseInt(urlProperties.getProperty(PROP_PORT));
        } catch (Exception e) {
            throw new SQLException("Port must be a valid number.");
        }
        if (port <= 0 || port > 65535) {
            throw new SQLException("Port is out of range: " + port);
        }
        int timeout;
        try {
            timeout = Integer.parseInt(urlProperties.getProperty(PROP_SOCKET_TIMEOUT));
        } catch (Exception e) {
            throw new SQLException("Timeout must be a valid number.");
        }
        if (timeout < 0) {
            throw new SQLException("Timeout must not be negative.");
        }
        return urlProperties;
    }

    protected SQLSocketProvider getSocketProviderInstance(String className) throws SQLException {
        SQLSocketProvider provider = providerCache.get(className);
        if (provider == null) {
            synchronized (this) {
                provider = providerCache.get(className);
                if (provider == null) {
                    try {
                        Class<?> cls = Class.forName(className);
                        if (SQLSocketProvider.class.isAssignableFrom(cls)) {
                            provider = (SQLSocketProvider) cls.newInstance();
                            providerCache.put(className, provider);
                        }
                    } catch (Exception e) {
                        throw new SQLException("Couldn't instantiate socket provider: " + className, e);
                    }
                }
            }
        }
        if (provider == null) {
            throw new SQLException(String.format("The socket provider %s does not implement %s",
                className, SQLSocketProvider.class.getCanonicalName()));
        }
        return provider;
    }

    @Override
    public boolean acceptsURL(String url) throws SQLException {
        return url.toLowerCase().startsWith("tarantool:");
    }

    @Override
    public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) throws SQLException {
        try {
            URI uri = new URI(url);
            Properties properties = parseQueryString(uri, info);

            DriverPropertyInfo host = new DriverPropertyInfo(PROP_HOST, properties.getProperty(PROP_HOST));
            host.required = true;
            host.description = "Tarantool server host";

            DriverPropertyInfo port = new DriverPropertyInfo(PROP_PORT, properties.getProperty(PROP_PORT));
            port.required = true;
            port.description = "Tarantool server port";

            DriverPropertyInfo user = new DriverPropertyInfo(PROP_USER, properties.getProperty(PROP_USER));
            user.required = false;
            user.description = "user";

            DriverPropertyInfo password = new DriverPropertyInfo(PROP_PASSWORD, properties.getProperty(PROP_PASSWORD));
            password.required = false;
            password.description = "password";

            DriverPropertyInfo socketProvider = new DriverPropertyInfo(
                PROP_SOCKET_PROVIDER, properties.getProperty(PROP_SOCKET_PROVIDER));

            socketProvider.required = false;
            socketProvider.description = "SocketProvider class implements org.tarantool.jdbc.SQLSocketProvider";

            DriverPropertyInfo socketTimeout = new DriverPropertyInfo(
                PROP_SOCKET_TIMEOUT, properties.getProperty(PROP_SOCKET_TIMEOUT));

            socketTimeout.required = false;
            socketTimeout.description = "The number of milliseconds to wait before a timeout is occurred on a socket" +
                " connect or read. The default value is 0, which means infinite timeout.";

            return new DriverPropertyInfo[] { host, port, user, password, socketProvider, socketTimeout };
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

    /**
     * Builds a string representation of given connection properties
     * along with their sanitized values.
     *
     * @param props Connection properties.
     *
     * @return Comma-separated pairs of property names and values.
     */
    protected static String diagProperties(Properties props) {
        StringBuilder sb = new StringBuilder();
        for (Map.Entry<Object, Object> e : props.entrySet()) {
            if (sb.length() > 0) {
                sb.append(", ");
            }
            sb.append(e.getKey());
            sb.append('=');
            sb.append((PROP_USER.equals(e.getKey()) || PROP_PASSWORD.equals(e.getKey()))
                ? "*****" : e.getValue().toString());
        }
        return sb.toString();
    }
}
