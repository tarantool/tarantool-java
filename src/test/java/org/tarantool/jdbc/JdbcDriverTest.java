package org.tarantool.jdbc;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.tarantool.jdbc.SQLDriver.PROP_HOST;
import static org.tarantool.jdbc.SQLDriver.PROP_PASSWORD;
import static org.tarantool.jdbc.SQLDriver.PROP_PORT;
import static org.tarantool.jdbc.SQLDriver.PROP_SOCKET_PROVIDER;
import static org.tarantool.jdbc.SQLDriver.PROP_SOCKET_TIMEOUT;
import static org.tarantool.jdbc.SQLDriver.PROP_USER;

import org.tarantool.CommunicationException;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.net.URI;
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
        prop.setProperty(PROP_USER, "adm");
        prop.setProperty(PROP_PASSWORD, "secret");

        URI uri = new URI(String.format(
            "tarantool://server.local:3302?%s=%s&%s=%d",
            PROP_SOCKET_PROVIDER, "some.class",
            PROP_SOCKET_TIMEOUT, 5000)
        );

        Properties res = drv.parseQueryString(uri, prop);
        assertNotNull(res);

        assertEquals("server.local", res.getProperty(PROP_HOST));
        assertEquals("3302", res.getProperty(PROP_PORT));
        assertEquals("adm", res.getProperty(PROP_USER));
        assertEquals("secret", res.getProperty(PROP_PASSWORD));
        assertEquals("some.class", res.getProperty(PROP_SOCKET_PROVIDER));
        assertEquals("5000", res.getProperty(PROP_SOCKET_TIMEOUT));
    }

    @Test
    public void testParseQueryStringUserInfoInURI() throws Exception {
        SQLDriver drv = new SQLDriver();
        Properties res = drv.parseQueryString(new URI("tarantool://adm:secret@server.local"), null);
        assertNotNull(res);
        assertEquals("server.local", res.getProperty(PROP_HOST));
        assertEquals("3301", res.getProperty(PROP_PORT));
        assertEquals("adm", res.getProperty(PROP_USER));
        assertEquals("secret", res.getProperty(PROP_PASSWORD));
    }

    @Test
    public void testParseQueryStringValidations() {
        // Check non-number port
        checkParseQueryStringValidation("tarantool://0",
            new Properties() {
                {
                    setProperty(PROP_PORT, "nan");
                }
            },
            "Port must be a valid number.");

        // Check zero port
        checkParseQueryStringValidation("tarantool://0:0", null, "Port is out of range: 0");

        // Check high port
        checkParseQueryStringValidation("tarantool://0:65536", null, "Port is out of range: 65536");

        // Check non-number timeout
        checkParseQueryStringValidation(String.format("tarantool://0:3301?%s=nan", PROP_SOCKET_TIMEOUT), null,
            "Timeout must be a valid number.");

        // Check negative timeout
        checkParseQueryStringValidation(String.format("tarantool://0:3301?%s=-100", PROP_SOCKET_TIMEOUT), null,
            "Timeout must not be negative.");
    }

    @Test
    public void testGetPropertyInfo() throws SQLException {
        Driver drv = new SQLDriver();
        Properties props = new Properties();
        DriverPropertyInfo[] info = drv.getPropertyInfo("tarantool://server.local:3302", props);
        assertNotNull(info);
        assertEquals(6, info.length);

        for (DriverPropertyInfo e : info) {
            assertNotNull(e.name);
            assertNull(e.choices);
            assertNotNull(e.description);

            if (PROP_HOST.equals(e.name)) {
                assertTrue(e.required);
                assertEquals("server.local", e.value);
            } else if (PROP_PORT.equals(e.name)) {
                assertTrue(e.required);
                assertEquals("3302", e.value);
            } else if (PROP_USER.equals(e.name)) {
                assertFalse(e.required);
                assertNull(e.value);
            } else if (PROP_PASSWORD.equals(e.name)) {
                assertFalse(e.required);
                assertNull(e.value);
            } else if (PROP_SOCKET_PROVIDER.equals(e.name)) {
                assertFalse(e.required);
                assertNull(e.value);
            } else if (PROP_SOCKET_TIMEOUT.equals(e.name)) {
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
            "The socket provider java.lang.Integer does not implement org.tarantool.jdbc.SQLSocketProvider");

        checkCustomSocketProviderFail(TestSQLProviderThatReturnsNull.class.getName(),
            "The socket provider returned null socket");

        checkCustomSocketProviderFail(TestSQLProviderThatThrows.class.getName(),
            "Couldn't initiate connection using");
    }

    @Test
    public void testNoResponseAfterInitialConnect() throws IOException {
        ServerSocket socket = new ServerSocket();
        socket.bind(null, 0);
        try {
            final String url = "tarantool://localhost:" + socket.getLocalPort();
            final Properties prop = new Properties();
            prop.setProperty(PROP_SOCKET_TIMEOUT, "100");
            SQLException e = assertThrows(SQLException.class, new Executable() {
                @Override
                public void execute() throws Throwable {
                    DriverManager.getConnection(url, prop);
                }
            });
            assertTrue(e.getMessage().startsWith("Couldn't initiate connection using "), e.getMessage());
            assertTrue(e.getCause() instanceof CommunicationException);
            assertTrue(e.getCause().getCause() instanceof SocketTimeoutException);
        } finally {
            socket.close();
        }
    }

    private void checkCustomSocketProviderFail(String providerClassName, String errMsg) throws SQLException {
        final Driver drv = DriverManager.getDriver("tarantool:");
        final Properties prop = new Properties();
        prop.setProperty(PROP_SOCKET_PROVIDER, providerClassName);

        SQLException e = assertThrows(SQLException.class, new Executable() {
            @Override
            public void execute() throws Throwable {
                drv.connect("tarantool://0:3301", prop);
            }
        });
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

    static class TestSQLProviderThatReturnsNull implements SQLSocketProvider {
        @Override
        public Socket getConnectedSocket(URI uri, Properties params) {
            return null;
        }
    }

    static class TestSQLProviderThatThrows implements SQLSocketProvider {
        @Override
        public Socket getConnectedSocket(URI uri, Properties params) {
            throw new RuntimeException("ERROR");
        }
    }
}
