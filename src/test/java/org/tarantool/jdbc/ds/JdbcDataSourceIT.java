package org.tarantool.jdbc.ds;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTimeout;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.tarantool.TestAssumptions.assumeMinimalServerVersion;

import org.tarantool.TarantoolTestHelper;
import org.tarantool.jdbc.SQLProperty;
import org.tarantool.util.ServerVersion;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.ThrowingSupplier;

import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.SQLException;
import java.time.Duration;
import javax.sql.DataSource;

@DisplayName("A data source")
class JdbcDataSourceIT {

    private static TarantoolTestHelper testHelper;

    private SQLDataSource dataSource;

    @BeforeAll
    static void setUpEnv() {
        testHelper = new TarantoolTestHelper("data-source-it");
        testHelper.createInstance();
        testHelper.startInstance();
    }

    @BeforeEach
    void setUp() {
        assumeMinimalServerVersion(testHelper.getInstanceVersion(), ServerVersion.V_2_1);
        dataSource = new SQLDataSource();
    }

    @AfterAll
    static void tearDownEnv() {
        testHelper.stopInstance();
    }

    @Test
    @DisplayName("returned an active connection with guest credentials")
    void testGetDefaultConnection() throws SQLException {
        Connection connection = dataSource.getConnection();
        assertTrue(connection.isValid(2));
        connection.close();
    }

    @Test
    @DisplayName("returned an active connection with user credentials")
    void testGetUserConnection() throws SQLException {
        Connection connection = dataSource.getConnection("test_admin", "4pWBZmLEgkmKK5WP");
        assertTrue(connection.isValid(2));
        connection.close();
    }

    @Test
    @DisplayName("gave a connection exceptionally with wrong user credentials")
    void testGetWrongUserConnection() throws SQLException {
        dataSource.setLoginTimeout(1);
        assertThrows(
            SQLException.class,
            () -> dataSource.getConnection("unknown_user", "wrong_pass")
        );
    }

    @Test
    @DisplayName("received a custom logger writer")
    void testLogWriter() throws Exception {
        assertNull(dataSource.getLogWriter());
        PrintWriter logWriter = new PrintWriter("testroot/ds-out.log");
        dataSource.setLogWriter(logWriter);
        assertEquals(logWriter, dataSource.getLogWriter());
    }

    @Test
    @DisplayName("timed out during connection establishment")
    void testLoginTimeout() throws SQLException {
        assertEquals(dataSource.getLoginTimeout(), 60);
        dataSource.setLoginTimeout(2);
        assertEquals(2, dataSource.getLoginTimeout());
        ThrowingSupplier<SQLException> badInit = () ->
            assertThrows(
                SQLException.class,
                () -> dataSource.getConnection("unknown_user", "wrong_pass")
            );

        assertTimeout(Duration.ofSeconds(4), badInit);
    }

    @Test
    @DisplayName("is compatible with proper interfaces")
    void testRightIsWrapperFor() {
        assertTrue(dataSource.isWrapperFor(DataSource.class));
        assertTrue(dataSource.isWrapperFor(TarantoolDataSource.class));
    }

    @Test
    @DisplayName("is not compatible with proper interfaces")
    void testWrongIsWrapperFor() {
        assertFalse(dataSource.isWrapperFor(Integer.class));
        assertFalse(dataSource.isWrapperFor(Driver.class));
    }

    @Test
    @DisplayName("unwrapped to proper interfaces")
    void testRightUnwrap() throws SQLException {
        assertNotNull(dataSource.unwrap(DataSource.class));
        assertNotNull(dataSource.unwrap(TarantoolDataSource.class));
    }

    @Test
    @DisplayName("unwrapped to proper interfaces")
    void testWrongUnwrap() {
        assertThrows(SQLException.class, () -> dataSource.unwrap(Integer.class));
        assertThrows(SQLException.class, () -> dataSource.unwrap(Driver.class));
    }

    @Test
    @DisplayName("was configured with a custom server name")
    void testServerNameProperty() {
        assertEquals(SQLProperty.HOST.getDefaultValue(), dataSource.getServerName());
        String expectedNewServerName = "my-server-name";
        dataSource.setServerName(expectedNewServerName);
        assertEquals(expectedNewServerName, dataSource.getServerName());
    }

    @Test
    @DisplayName("was configured with a custom port number")
    void testPortNumberProperty() throws SQLException {
        assertEquals(SQLProperty.PORT.getDefaultIntValue(), dataSource.getPortNumber());
        int expectedNewPort = 4001;
        dataSource.setPortNumber(expectedNewPort);
        assertEquals(expectedNewPort, dataSource.getPortNumber());
    }

    @Test
    @DisplayName("was configured with a custom user name")
    void testUserProperty() {
        assertEquals(SQLProperty.USER.getDefaultValue(), dataSource.getUser());
        String expectedNewUser = "myUserName";
        dataSource.setUser(expectedNewUser);
        assertEquals(expectedNewUser, dataSource.getUser());
    }

    @Test
    @DisplayName("was configured with a custom user password")
    void testPasswordProperty() {
        assertEquals(SQLProperty.PASSWORD.getDefaultValue(), dataSource.getPassword());
        String expectedNewPassword = "newPassword";
        dataSource.setPassword(expectedNewPassword);
        assertEquals(expectedNewPassword, dataSource.getPassword());
    }

    @Test
    @DisplayName("was configured with a custom channel socket provider")
    void testSocketChannelProviderProperty() {
        assertEquals(SQLProperty.SOCKET_CHANNEL_PROVIDER.getDefaultValue(), dataSource.getSocketChannelProvider());
        String expectedProviderClassName = "z.x.z.MyClass";
        dataSource.setSocketChannelProvider(expectedProviderClassName);
        assertEquals(expectedProviderClassName, dataSource.getSocketChannelProvider());
    }

    @Test
    @DisplayName("was configured with a custom query timeout")
    void testRequestTimeoutProperty() throws SQLException {
        assertEquals(SQLProperty.QUERY_TIMEOUT.getDefaultIntValue(), dataSource.getQueryTimeout());
        int expectedNewTimeout = 45;
        dataSource.setQueryTimeout(expectedNewTimeout);
        assertEquals(expectedNewTimeout, dataSource.getQueryTimeout());
    }

    @Test
    @DisplayName("was configured with a custom data source name")
    void testDataSourceNameProperty() {
        String expectedDataSourceName = "dataSourceName";
        dataSource.setDataSourceName(expectedDataSourceName);
        assertEquals(expectedDataSourceName, dataSource.getDataSourceName());
    }

}
