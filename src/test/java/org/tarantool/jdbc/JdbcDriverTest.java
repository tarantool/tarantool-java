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

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;

import java.io.IOException;
import java.net.ServerSocket;
import java.nio.channels.SocketChannel;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.util.Properties;

public class JdbcDriverTest {

    private SQLDriver driver;

    @BeforeEach
    void setUp() {
        driver = new SQLDriver();
    }

    @Test
    public void testParseQueryString() throws Exception {
        Properties prop = new Properties();
        SQLProperty.USER.setString(prop, "adm");
        SQLProperty.PASSWORD.setString(prop, "secret");

        String uri = String.format(
            "jdbc:tarantool://server.local:3302?%s=%s&%s=%d",
            SQLProperty.SOCKET_CHANNEL_PROVIDER.getName(),
            "some.class",
            SQLProperty.QUERY_TIMEOUT.getName(),
            5000
        );

        Properties result = driver.parseConnectionString(uri, prop);
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
        Properties result = driver.parseConnectionString("jdbc:tarantool://adm:secret@server.local", null);
        assertNotNull(result);
        assertEquals("server.local", SQLProperty.HOST.getString(result));
        assertEquals("3301", SQLProperty.PORT.getString(result));
        assertEquals("adm", SQLProperty.USER.getString(result));
        assertEquals("secret", SQLProperty.PASSWORD.getString(result));
    }

    @Test
    public void testParseWrongURL() throws Exception {
        checkParseQueryStringValidation(
            "jdbc:tarantool:adm:secret@server.local",
            null,
            "Invalid URL: '//' is not presented."
        );
    }

    @Test
    public void testParseMultiHostURI() throws Exception {
        Properties result = driver.parseConnectionString(
            "jdbc:tarantool://user:pwd@server.local,localhost:3301,192.168.0.1:3302",
            null
        );
        assertNotNull(result);
        assertEquals("server.local,localhost,192.168.0.1", SQLProperty.HOST.getString(result));
        assertEquals("3301,3301,3302", SQLProperty.PORT.getString(result));
        assertEquals("user", SQLProperty.USER.getString(result));
        assertEquals("pwd", SQLProperty.PASSWORD.getString(result));
    }

    @Test
    public void testParseWrongPort() {
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
    }

    @Test
    void testParseInvalidNumbers() {
        SQLProperty[] properties = {
            SQLProperty.LOGIN_TIMEOUT,
            SQLProperty.QUERY_TIMEOUT,
            SQLProperty.CLUSTER_DISCOVERY_DELAY_MILLIS
        };
        for (SQLProperty property : properties) {
            checkParseQueryStringValidation(
                String.format("jdbc:tarantool://0:3301?%s=nan", property.getName()),
                null,
                String.format("Property %s must be a valid number.", property.getName())
            );
            checkParseQueryStringValidation(
                String.format("jdbc:tarantool://0:3301?%s=-100", property.getName()),
                null,
                String.format("Property %s must not be negative.", property.getName())
            );
        }
    }

    @Test
    public void testGetPropertyInfo() throws SQLException {
        Properties props = new Properties();
        DriverPropertyInfo[] info = driver.getPropertyInfo("jdbc:tarantool://server.local:3302", props);
        assertNotNull(info);
        assertEquals(9, info.length);

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
            } else if (SQLProperty.CLUSTER_DISCOVERY_ENTRY_FUNCTION.getName().equals(e.name)) {
                assertFalse(e.required);
                assertNull(e.value);
            } else if (SQLProperty.CLUSTER_DISCOVERY_DELAY_MILLIS.getName().equals(e.name)) {
                assertFalse(e.required);
                assertEquals("60000", e.value);
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

    @Test
    void testAcceptUrl() throws SQLException {
        assertFalse(driver.acceptsURL("http://localhost"));
        assertFalse(driver.acceptsURL("jdbc:mysql://host1/myDb"));
        assertTrue(driver.acceptsURL("jdbc:tarantool://localhost:3301"));
        assertThrows(SQLException.class, () -> driver.acceptsURL(null));
    }

    private void checkCustomSocketProviderFail(String providerClassName, String errMsg) throws SQLException {
        final Driver drv = DriverManager.getDriver("jdbc:tarantool:");
        final Properties prop = new Properties();
        SQLProperty.SOCKET_CHANNEL_PROVIDER.setString(prop, providerClassName);
        SQLProperty.LOGIN_TIMEOUT.setInt(prop, 500);

        SQLException e = assertThrows(SQLException.class, () -> drv.connect("jdbc:tarantool://0:3301", prop));
        assertTrue(e.getMessage().startsWith(errMsg), e.getMessage());
    }

    private void checkParseQueryStringValidation(final String uri, final Properties properties, String error) {
        SQLException e = assertThrows(SQLException.class, () -> driver.parseConnectionString(uri, properties));
        assertTrue(e.getMessage().startsWith(error), e.getMessage());
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
