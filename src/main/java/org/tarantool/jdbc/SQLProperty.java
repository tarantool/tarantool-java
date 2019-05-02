package org.tarantool.jdbc;

import org.tarantool.util.SQLStates;

import java.sql.SQLException;
import java.util.Properties;

public enum SQLProperty {
    HOST(
        "host",
        "Tarantool server host",
        "localhost",
        null,
        true
    ),
    PORT(
        "port",
        "Tarantool server port",
        "3301",
        null,
        true
    ),
    SOCKET_CHANNEL_PROVIDER(
        "socketChannelProvider",
        "SocketProvider class implements org.tarantool.SocketChannelProvider",
        null,
        null,
        false
    ),
    USER(
        "user",
        "Username to connect",
        null,
        null,
        false
    ),
    PASSWORD(
        "password",
        "User password to connect",
        null,
        null,
        false
    ),
    LOGIN_TIMEOUT(
        "loginTimeout",
        "The number of milliseconds to wait for connection establishment. " +
            "The default value is 60000 (1 minute).",
        "60000",
        null,
        false
    ),
    QUERY_TIMEOUT(
        "queryTimeout",
        "The number of milliseconds to wait before a timeout is occurred for the query. " +
            "The default value is 0 (infinite) timeout.",
        "0",
        null,
        false
    );

    private final String name;
    private final String description;
    private final String defaultValue;
    private final String[] choices;
    private final boolean required;

    SQLProperty(String name, String description, String defaultValue, String[] choices, boolean required) {
        this.name = name;
        this.description = description;
        this.defaultValue = defaultValue;
        this.choices = choices;
        this.required = required;
    }

    public String getName() {
        return name;
    }

    public String getDescription() {
        return description;
    }

    public String getDefaultValue() {
        return defaultValue;
    }

    public boolean isRequired() {
        return required;
    }

    public String[] getChoices() {
        return choices;
    }

    public String getString(Properties properties) {
        return properties.getProperty(name, defaultValue);
    }

    public void setString(Properties properties, String value) {
        if (value == null) {
            properties.remove(name);
        } else {
            properties.setProperty(name, value);
        }
    }

    public int getInt(Properties properties) throws SQLException {
        String property = getString(properties);
        try {
            return Integer.parseInt(property);
        } catch (NumberFormatException exception) {
            throw new SQLException(
                "Property " + name + " must be a valid number.",
                SQLStates.INVALID_PARAMETER_VALUE.getSqlState(),
                exception
            );
        }
    }

    public void setInt(Properties properties, int value) {
        setString(properties, Integer.toString(value));
    }
}
