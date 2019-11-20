package org.tarantool.jdbc;

import org.tarantool.SocketChannelProvider;
import org.tarantool.TarantoolClusterClientConfig;
import org.tarantool.util.NodeSpec;
import org.tarantool.util.SQLStates;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Logger;

public class SQLDriver implements Driver {

    private static final String TARANTOOL_JDBC_SCHEME = "jdbc:tarantool:";

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
        if (!acceptsURL(url)) {
            return null;
        }

        final Properties urlProperties = parseConnectionString(url, info);

        String[] hosts = SQLProperty.HOST.getString(urlProperties).split(",");
        String[] ports = SQLProperty.PORT.getString(urlProperties).split(",");
        List<NodeSpec> nodes = new ArrayList<>(hosts.length);
        for (int i = 0; i < hosts.length; i++) {
            nodes.add(new NodeSpec(hosts[i], Integer.valueOf(ports[i])));
        }

        String providerClassName = SQLProperty.SOCKET_CHANNEL_PROVIDER.getString(urlProperties);
        if (providerClassName == null) {
            return new SQLConnection(url, nodes, urlProperties);
        }
        final SocketChannelProvider provider = getSocketProviderInstance(providerClassName);
        return new SQLConnection(url, nodes, urlProperties) {
            @Override
            protected SQLTarantoolClientImpl makeSqlClient(List<String> addresses,
                                                           TarantoolClusterClientConfig config) {
                return new SQLTarantoolClientImpl(provider, config);
            }
        };
    }

    /**
     * Parses an URL and to parameters and merges
     * they with the parameters provided. If same
     * parameter specified in both URL and properties
     * the
     *
     * <p>
     * jdbc:tarantool://[user-info@][nodes][?parameters]
     * user-info ::= user[:password]
     * nodes ::= host1[:port1][,host2[:port2] ... ]
     * parameters ::= param1=value1[&param2=value2 ... ]
     *
     * @param url  target URL string
     * @param info extra properties to be merged
     *
     * @return merged properties
     *
     * @throws SQLException if any invalid parameters are passed
     */
    protected Properties parseConnectionString(String url, Properties info) throws SQLException {
        Properties urlProperties = new Properties();

        String schemeSpecificPart = url.substring(TARANTOOL_JDBC_SCHEME.length());
        if (!schemeSpecificPart.startsWith("//")) {
            throw new SQLException("Invalid URL: '//' is not presented.");
        }
        int userInfoEndPosition = schemeSpecificPart.indexOf('@');
        int queryStartPosition = schemeSpecificPart.indexOf('?');

        if (userInfoEndPosition != -1) {
            parseUserInfo(schemeSpecificPart.substring(2, userInfoEndPosition), urlProperties);
        }
        if (queryStartPosition != -1) {
            parseQueryParameters(schemeSpecificPart.substring(queryStartPosition + 1), urlProperties);
        }
        String nodesPart = schemeSpecificPart.substring(
            userInfoEndPosition == -1 ? 2 : userInfoEndPosition + 1,
            queryStartPosition == -1 ? schemeSpecificPart.length() : queryStartPosition
        );
        parseNodes(nodesPart, urlProperties);

        if (info != null) {
            urlProperties.putAll(info);
        }

        requirePortNumber(SQLProperty.PORT, urlProperties);
        requireNonNegativeInteger(SQLProperty.LOGIN_TIMEOUT, urlProperties);
        requireNonNegativeInteger(SQLProperty.QUERY_TIMEOUT, urlProperties);
        requireNonNegativeInteger(SQLProperty.CLUSTER_DISCOVERY_DELAY_MILLIS, urlProperties);

        return urlProperties;
    }

    private void parseUserInfo(String userInfo, Properties properties) {
        int i = userInfo.indexOf(':');
        if (i < 0) {
            SQLProperty.USER.setString(properties, userInfo);
        } else {
            SQLProperty.USER.setString(properties, userInfo.substring(0, i));
            SQLProperty.PASSWORD.setString(properties, userInfo.substring(i + 1));
        }
    }

    private void parseNodes(String nodesPart, Properties properties) throws SQLException {
        String[] nodes = nodesPart.split(",");
        StringBuilder hosts = new StringBuilder();
        StringBuilder ports = new StringBuilder();
        for (String node : nodes) {
            int portIndex = node.lastIndexOf(':');
            if (portIndex != -1 && node.lastIndexOf(']') < portIndex) {
                String portString = node.substring(portIndex + 1);
                hosts.append(node, 0, portIndex);
                ports.append(portString);
            } else {
                hosts.append(node);
                ports.append(SQLProperty.PORT.getDefaultValue());
            }
            hosts.append(',');
            ports.append(',');
        }

        hosts.setLength(hosts.length() - 1);
        ports.setLength(ports.length() - 1);
        SQLProperty.HOST.setString(properties, hosts.toString());
        SQLProperty.PORT.setString(properties, ports.toString());
    }

    private void parseQueryParameters(String queryParams, Properties properties) throws SQLException {
        String[] parts = queryParams.split("&");
        for (String part : parts) {
            int i = part.indexOf("=");
            if (i > -1) {
                String name = part.substring(0, i);
                String value = null;
                try {
                    value = URLDecoder.decode(part.substring(i + 1), "UTF-8");
                } catch (UnsupportedEncodingException cause) {
                    throw new SQLException(cause.getMessage(), SQLStates.INVALID_PARAMETER_VALUE.getSqlState(), cause);
                }
                properties.put(name, value);
            } else {
                properties.put(part, "");
            }
        }
    }

    private void requirePortNumber(SQLProperty sqlProperty, Properties properties) throws SQLException {
        String[] portList = sqlProperty.getString(properties).split(",");
        for (String portString : portList) {
            try {
                int port = Integer.parseInt(portString);
                if (port < 1 || port > 65535) {
                    throw new SQLException(
                        "Port is out of range: " + port, SQLStates.INVALID_PARAMETER_VALUE.getSqlState()
                    );
                }
            } catch (NumberFormatException cause) {
                throw new SQLException(
                    "Property " + sqlProperty.getName() + " must be a valid number.",
                    SQLStates.INVALID_PARAMETER_VALUE.getSqlState(),
                    cause
                );
            }
        }
    }

    private void requireNonNegativeInteger(SQLProperty sqlProperty, Properties properties) throws SQLException {
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
        if (url == null) {
            throw new SQLException("Url must not be null");
        }
        return url.startsWith(TARANTOOL_JDBC_SCHEME);
    }

    @Override
    public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) throws SQLException {
        Properties properties = parseConnectionString(url, info);
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
