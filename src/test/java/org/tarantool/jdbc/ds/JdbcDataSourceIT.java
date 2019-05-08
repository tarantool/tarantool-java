package org.tarantool.jdbc.ds;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTimeout;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.tarantool.TestUtils.makeInstanceEnv;

import org.tarantool.TarantoolControl;
import org.tarantool.jdbc.SQLProperty;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.ThrowingSupplier;

import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.SQLException;
import java.time.Duration;
import javax.sql.DataSource;

class JdbcDataSourceIT {

    private static final String LUA_FILE = "jdk-testing.lua";
    private static final int LISTEN = 3301;
    private static final int ADMIN = 3313;
    private static final String INSTANCE_NAME = "data-source-testing";

    private SQLDataSource dataSource;

    @BeforeEach
    void setUp() {
        TarantoolControl control = new TarantoolControl();
        control.createInstance(INSTANCE_NAME, LUA_FILE, makeInstanceEnv(LISTEN, ADMIN));
        control.start(INSTANCE_NAME);

        dataSource = new SQLDataSource();
    }

    @AfterEach
    void tearDown() {
        TarantoolControl control = new TarantoolControl();
        control.stop(INSTANCE_NAME);
    }

    @Test
    void testGetDefaultConnection() throws SQLException {
        Connection connection = dataSource.getConnection();
        assertTrue(connection.isValid(2));
        connection.close();
    }

    @Test
    void testGetUserConnection() throws SQLException {
        Connection connection = dataSource.getConnection("test_admin", "4pWBZmLEgkmKK5WP");
        assertTrue(connection.isValid(2));
        connection.close();
    }

    @Test
    void testGetWrongUserConnection() throws SQLException {
        dataSource.setLoginTimeout(1);
        assertThrows(
            SQLException.class,
            () -> dataSource.getConnection("unknown_user", "wrong_pass")
        );
    }

    @Test
    void testLogWriter() throws Exception {
        assertNull(dataSource.getLogWriter());
        PrintWriter logWriter = new PrintWriter("jdk-testing.lua");
        dataSource.setLogWriter(logWriter);
        assertEquals(logWriter, dataSource.getLogWriter());
    }

    @Test
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
    void testRightIsWrapperFor() {
        assertTrue(dataSource.isWrapperFor(DataSource.class));
        assertTrue(dataSource.isWrapperFor(TarantoolDataSource.class));
    }

    @Test
    void testWrongIsWrapperFor() {
        assertFalse(dataSource.isWrapperFor(Integer.class));
        assertFalse(dataSource.isWrapperFor(Driver.class));
    }

    @Test
    void testRightUnwrap() throws SQLException {
        assertNotNull(dataSource.unwrap(DataSource.class));
        assertNotNull(dataSource.unwrap(TarantoolDataSource.class));
    }

    @Test
    void testWrongUnwrap() {
        assertThrows(SQLException.class, () -> dataSource.unwrap(Integer.class));
        assertThrows(SQLException.class, () -> dataSource.unwrap(Driver.class));
    }

    @Test
    void testServerNameProperty() {
        assertEquals(SQLProperty.HOST.getDefaultValue(), dataSource.getServerName());
        String expectedNewServerName = "my-server-name";
        dataSource.setServerName(expectedNewServerName);
        assertEquals(expectedNewServerName, dataSource.getServerName());
    }

    @Test
    void testPortNumberProperty() throws SQLException {
        assertEquals(SQLProperty.PORT.getDefaultIntValue(), dataSource.getPortNumber());
        int expectedNewPort = 4001;
        dataSource.setPortNumber(expectedNewPort);
        assertEquals(expectedNewPort, dataSource.getPortNumber());
    }

    @Test
    void testUserProperty() {
        assertEquals(SQLProperty.USER.getDefaultValue(), dataSource.getUser());
        String expectedNewUser = "myUserName";
        dataSource.setUser(expectedNewUser);
        assertEquals(expectedNewUser, dataSource.getUser());
    }

    @Test
    void testPasswordProperty() {
        assertEquals(SQLProperty.PASSWORD.getDefaultValue(), dataSource.getPassword());
        String expectedNewPassword = "newPassword";
        dataSource.setPassword(expectedNewPassword);
        assertEquals(expectedNewPassword, dataSource.getPassword());
    }

    @Test
    void testSocketChannelProviderProperty() {
        assertEquals(SQLProperty.SOCKET_CHANNEL_PROVIDER.getDefaultValue(), dataSource.getSocketChannelProvider());
        String expectedProviderClassName = "z.x.z.MyClass";
        dataSource.setSocketChannelProvider(expectedProviderClassName);
        assertEquals(expectedProviderClassName, dataSource.getSocketChannelProvider());
    }

    @Test
    void testRequestTimeoutProperty() throws SQLException {
        assertEquals(SQLProperty.QUERY_TIMEOUT.getDefaultIntValue(), dataSource.getQueryTimeout());
        int expectedNewTimeout = 45;
        dataSource.setQueryTimeout(expectedNewTimeout);
        assertEquals(expectedNewTimeout, dataSource.getQueryTimeout());
    }

    @Test
    void testDataSourceNameProperty() {
        String expectedDataSourceName = "dataSourceName";
        dataSource.setDataSourceName(expectedDataSourceName);
        assertEquals(expectedDataSourceName, dataSource.getDataSourceName());
    }

}
