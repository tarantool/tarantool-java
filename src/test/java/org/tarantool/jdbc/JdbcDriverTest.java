package org.tarantool.jdbc;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import org.tarantool.CommunicationException;
import org.tarantool.SocketChannelProvider;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.URI;
import java.nio.channels.SocketChannel;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.util.Properties;

public class JdbcDriverTest {

    @Test
    public void testParseQueryString() throws Exception {
        SQLDriver drv = new SQLDriver();

        Properties prop = new Properties();
        SQLProperty.USER.setString(prop, "adm");
        SQLProperty.PASSWORD.setString(prop, "secret");

        URI uri = new URI(String.format(
            "jdbc:tarantool://server.local:3302?%s=%s&%s=%d",
            SQLProperty.SOCKET_CHANNEL_PROVIDER.getName(), "some.class",
            SQLProperty.QUERY_TIMEOUT.getName(), 5000)
        );

        Properties result = drv.parseQueryString(uri, prop);
        assertNotNull(result);

        assertEquals("server.local", SQLProperty.HOST.getString(result));
        assertEquals("3302", SQLProperty.PORT.getString(result));
        assertEquals("adm", SQLProperty.USER.getString(result));
        assertEquals("secret", SQLProperty.PASSWORD.getString(result));
        assertEquals("some.class", SQLProperty.SOCKET_CHANNEL_PROVIDER.getString(result));
        assertEquals("5000", SQLProperty.QUERY_TIMEOUT.getString(result));
    }

    @Test
    public void testParseQueryStringUserInfoInURI() throws Exception {
        SQLDriver drv = new SQLDriver();
        Properties result = drv.parseQueryString(new URI("jdbc:tarantool://adm:secret@server.local"), null);
        assertNotNull(result);
        assertEquals("server.local", SQLProperty.HOST.getString(result));
        assertEquals("3301", SQLProperty.PORT.getString(result));
        assertEquals("adm", SQLProperty.USER.getString(result));
        assertEquals("secret", SQLProperty.PASSWORD.getString(result));
    }

    @Test
    public void testParseQueryStringValidations() {
        // Check non-number port
        checkParseQueryStringValidation("jdbc:tarantool://0",
            new Properties() {
                {
                    SQLProperty.PORT.setString(this, "nan");
                }
            },
            "Property port must be a valid number.");

        // Check zero port
        checkParseQueryStringValidation("jdbc:tarantool://0:0", null, "Port is out of range: 0");

        // Check high port
        checkParseQueryStringValidation("jdbc:tarantool://0:65536", null, "Port is out of range: 65536");

        // Check non-number init timeout
        checkParseQueryStringValidation(
            String.format("jdbc:tarantool://0:3301?%s=nan", SQLProperty.LOGIN_TIMEOUT.getName()),
            null,
            "Property loginTimeout must be a valid number."
        );

        // Check negative init timeout
        checkParseQueryStringValidation(
            String.format("jdbc:tarantool://0:3301?%s=-100", SQLProperty.LOGIN_TIMEOUT.getName()),
            null,
            "Property loginTimeout must not be negative."
        );

        // Check non-number operation timeout
        checkParseQueryStringValidation(
            String.format("jdbc:tarantool://0:3301?%s=nan", SQLProperty.QUERY_TIMEOUT.getName()),
            null,
            "Property queryTimeout must be a valid number."
        );

        // Check negative operation timeout
        checkParseQueryStringValidation(
            String.format("jdbc:tarantool://0:3301?%s=-100", SQLProperty.QUERY_TIMEOUT.getName()),
            null,
            "Property queryTimeout must not be negative."
        );
    }

    @Test
    public void testGetPropertyInfo() throws SQLException {
        Driver drv = new SQLDriver();
        Properties props = new Properties();
        DriverPropertyInfo[] info = drv.getPropertyInfo("jdbc:tarantool://server.local:3302", props);
        assertNotNull(info);
        assertEquals(7, info.length);

        for (DriverPropertyInfo e : info) {
            assertNotNull(e.name);
            assertNull(e.choices);
            assertNotNull(e.description);

            if (SQLProperty.HOST.getName().equals(e.name)) {
                assertTrue(e.required);
                assertEquals("server.local", e.value);
            } else if (SQLProperty.PORT.getName().equals(e.name)) {
                assertTrue(e.required);
                assertEquals("3302", e.value);
            } else if (SQLProperty.USER.getName().equals(e.name)) {
                assertFalse(e.required);
                assertNull(e.value);
            } else if (SQLProperty.PASSWORD.getName().equals(e.name)) {
                assertFalse(e.required);
                assertNull(e.value);
            } else if (SQLProperty.SOCKET_CHANNEL_PROVIDER.getName().equals(e.name)) {
                assertFalse(e.required);
                assertNull(e.value);
            } else if (SQLProperty.LOGIN_TIMEOUT.getName().equals(e.name)) {
                assertFalse(e.required);
                assertEquals("60000", e.value);
            } else if (SQLProperty.QUERY_TIMEOUT.getName().equals(e.name)) {
                assertFalse(e.required);
                assertEquals("0", e.value);
            } else {
                fail("Unknown property '" + e.name + "'");
            }
        }
    }

    @Test
    public void testCustomSocketProviderFail() throws SQLException {
        checkCustomSocketProviderFail("nosuchclassexists",
            "Couldn't instantiate socket provider");

        checkCustomSocketProviderFail(Integer.class.getName(),
            "The socket provider java.lang.Integer does not implement org.tarantool.SocketChannelProvider");

        checkCustomSocketProviderFail(TestSQLProviderThatReturnsNull.class.getName(),
            "Couldn't initiate connection using");

        checkCustomSocketProviderFail(TestSQLProviderThatThrows.class.getName(),
            "Couldn't initiate connection using");
    }

    @Test
    public void testNoResponseAfterInitialConnect() throws IOException {
        ServerSocket socket = new ServerSocket();
        socket.bind(null, 0);
        try {
            final String url = "jdbc:tarantool://localhost:" + socket.getLocalPort();
            final Properties prop = new Properties();
            SQLProperty.LOGIN_TIMEOUT.setInt(prop, 500);
            SQLException e = assertThrows(SQLException.class, new Executable() {
                @Override
                public void execute() throws Throwable {
                    DriverManager.getConnection(url, prop);
                }
            });
            assertTrue(e.getMessage().startsWith("Couldn't initiate connection using "), e.getMessage());
            assertTrue(e.getCause() instanceof CommunicationException);
        } finally {
            socket.close();
        }
    }

    private void checkCustomSocketProviderFail(String providerClassName, String errMsg) throws SQLException {
        final Driver drv = DriverManager.getDriver("jdbc:tarantool:");
        final Properties prop = new Properties();
        SQLProperty.SOCKET_CHANNEL_PROVIDER.setString(prop, providerClassName);
        SQLProperty.LOGIN_TIMEOUT.setInt(prop, 500);

        SQLException e = assertThrows(SQLException.class, () -> drv.connect("jdbc:tarantool://0:3301", prop));
        assertTrue(e.getMessage().startsWith(errMsg), e.getMessage());
    }

    private void checkParseQueryStringValidation(final String uri, final Properties prop, String errMsg) {
        final SQLDriver drv = new SQLDriver();
        SQLException e = assertThrows(SQLException.class, new Executable() {
            @Override
            public void execute() throws Throwable {
                drv.parseQueryString(new URI(uri), prop);
            }
        });
        assertTrue(e.getMessage().startsWith(errMsg), e.getMessage());
    }

    static class TestSQLProviderThatReturnsNull implements SocketChannelProvider {

        @Override
        public SocketChannel get(int retryNumber, Throwable lastError) {
            return null;
        }

    }

    static class TestSQLProviderThatThrows implements SocketChannelProvider {

        @Override
        public SocketChannel get(int retryNumber, Throwable lastError) {
            throw new RuntimeException("ERROR");
        }

    }
}
