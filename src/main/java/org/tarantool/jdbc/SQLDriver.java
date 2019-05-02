package org.tarantool.jdbc;

import org.tarantool.SocketChannelProvider;
import org.tarantool.TarantoolClientConfig;
import org.tarantool.util.SQLStates;

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

    private final Map<String, SocketChannelProvider> providerCache = new ConcurrentHashMap<>();

    @Override
    public Connection connect(String url, Properties info) throws SQLException {
        if (url == null) {
            throw new SQLException("Url must not be null");
        }
        if (!acceptsURL(url)) {
            return null;
        }

        final URI uri = URI.create(url);
        final Properties urlProperties = parseQueryString(uri, info);
        String providerClassName = SQLProperty.SOCKET_CHANNEL_PROVIDER.getString(urlProperties);

        if (providerClassName == null) {
            return new SQLConnection(url, urlProperties);
        }

        final SocketChannelProvider provider = getSocketProviderInstance(providerClassName);

        return new SQLConnection(url, urlProperties) {
            @Override
            protected SQLTarantoolClientImpl makeSqlClient(String address, TarantoolClientConfig config) {
                return new SQLTarantoolClientImpl(provider, config);
            }
        };
    }

    protected Properties parseQueryString(URI uri, Properties info) throws SQLException {
        Properties urlProperties = new Properties();

        String userInfo = uri.getUserInfo();
        if (userInfo != null) {
            // Get user and password from the corresponding part of the URI, i.e. before @ sign.
            int i = userInfo.indexOf(':');
            if (i < 0) {
                SQLProperty.USER.setString(urlProperties, userInfo);
            } else {
                SQLProperty.USER.setString(urlProperties, userInfo.substring(0, i));
                SQLProperty.PASSWORD.setString(urlProperties, userInfo.substring(i + 1));
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
            urlProperties.setProperty(SQLProperty.HOST.getName(), uri.getHost());
        }
        if (uri.getPort() >= 0) {
            // We need to convert port to string otherwise getProperty() will not see it.
            urlProperties.setProperty(SQLProperty.PORT.getName(), String.valueOf(uri.getPort()));
        }
        if (info != null) {
            urlProperties.putAll(info);
        }

        // Validate properties.
        int port = SQLProperty.PORT.getInt(urlProperties);
        if (port <= 0 || port > 65535) {
            throw new SQLException("Port is out of range: " + port, SQLStates.INVALID_PARAMETER_VALUE.getSqlState());
        }

        checkTimeout(SQLProperty.LOGIN_TIMEOUT, urlProperties);
        checkTimeout(SQLProperty.QUERY_TIMEOUT, urlProperties);

        return urlProperties;
    }

    private void checkTimeout(SQLProperty sqlProperty, Properties properties) throws SQLException {
        int timeout = sqlProperty.getInt(properties);
        if (timeout < 0) {
            throw new SQLException(
                "Property " + sqlProperty.getName() + " must not be negative.",
                SQLStates.INVALID_PARAMETER_VALUE.getSqlState()
            );
        }
    }

    protected SocketChannelProvider getSocketProviderInstance(String className) throws SQLException {
        SocketChannelProvider provider = providerCache.get(className);
        if (provider == null) {
            synchronized (this) {
                provider = providerCache.get(className);
                if (provider == null) {
                    try {
                        Class<?> cls = Class.forName(className);
                        if (SocketChannelProvider.class.isAssignableFrom(cls)) {
                            provider = (SocketChannelProvider) cls.getDeclaredConstructor().newInstance();
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
                className, SocketChannelProvider.class.getCanonicalName()));
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

            SQLProperty[] sqlProperties = SQLProperty.values();
            DriverPropertyInfo[] propertyInfoList = new DriverPropertyInfo[sqlProperties.length];
            for (int i = 0; i < sqlProperties.length; i++) {
                SQLProperty sqlProperty = sqlProperties[i];
                String value = sqlProperty.getString(properties);
                DriverPropertyInfo propertyInfo = new DriverPropertyInfo(sqlProperty.getName(), value);
                propertyInfo.required = sqlProperty.isRequired();
                propertyInfo.description = sqlProperty.getDescription();
                propertyInfo.choices = sqlProperty.getChoices();
                propertyInfoList[i] = propertyInfo;
            }
            return propertyInfoList;
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
        String userProp = SQLProperty.USER.getName();
        String passwordProp = SQLProperty.PASSWORD.getName();

        StringBuilder sb = new StringBuilder();
        for (Map.Entry<Object, Object> e : props.entrySet()) {
            if (sb.length() > 0) {
                sb.append(", ");
            }
            sb.append(e.getKey());
            sb.append('=');
            sb.append(
                (userProp.equals(e.getKey()) || passwordProp.equals(e.getKey()))
                    ? "*****"
                    : e.getValue().toString()
            );
        }
        return sb.toString();
    }

}
