package org.tarantool.jdbc;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.tarantool.TestAssumptions.assumeMinimalServerVersion;

import org.tarantool.ServerVersion;
import org.tarantool.TarantoolTestHelper;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.security.Permission;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.EnumSet;
import java.util.concurrent.Executors;

public class JdbcSecurityIT {

    private static TarantoolTestHelper testHelper;

    private Connection connection;
    private SecurityManager originalSecurityManager;

    @BeforeAll
    public static void setupEnv() {
        testHelper = new TarantoolTestHelper("jdbc-security-it");
        testHelper.createInstance();
        testHelper.startInstance();
    }

    @AfterAll
    public static void teardownEnv() {
        testHelper.stopInstance();
    }

    @BeforeEach
    public void setUpTest() throws SQLException {
        assumeMinimalServerVersion(testHelper.getInstanceVersion(), ServerVersion.V_2_1);
        connection = DriverManager.getConnection(SqlTestUtils.makeDefaultJdbcUrl());
        originalSecurityManager = System.getSecurityManager();
    }

    @AfterEach
    public void tearDownTest() throws SQLException {
        assumeMinimalServerVersion(testHelper.getInstanceVersion(), ServerVersion.V_2_1);
        if (connection != null) {
            connection.close();
        }
        System.setSecurityManager(originalSecurityManager);
    }

    @Test
    void testDeniedConnectionAbort() {
        EnumSet<JdbcPermission> exclusions = EnumSet.of(JdbcPermission.CALL_ABORT);
        System.setSecurityManager(new JdbcSecurityManager(true, exclusions));

        SecurityException securityException = assertThrows(
            SecurityException.class,
            () -> connection.abort(Executors.newSingleThreadExecutor())
        );
        assertEquals(securityException.getMessage(), "Permission callAbort is not allowed");
    }

    @Test
    void testDeniedSetConnectionTimeout() {
        EnumSet<JdbcPermission> exclusions = EnumSet.of(JdbcPermission.SET_NETWORK_TIMEOUT);
        System.setSecurityManager(new JdbcSecurityManager(true, exclusions));

        SecurityException securityException = assertThrows(
            SecurityException.class,
            () -> connection.setNetworkTimeout(Executors.newSingleThreadExecutor(), 1000)
        );
        assertEquals(securityException.getMessage(), "Permission setNetworkTimeout is not allowed");
    }

    /**
     * Lists permissions supported by JDBC API.
     *
     * <ul>
     *      <li>setLog</li>
     *      <li>callAbort</li>
     *      <li>setSyncFactory<</li>
     *      <li>setNetworkTimeout</li>
     *      <li>deregisterDriver</li>
     * </ul>
     *
     * @see java.sql.SQLPermission
     */
    private enum JdbcPermission {
        SET_LOG("setLog"),
        CALL_ABORT("callAbort"),
        SET_SYNC_FACTORY("setSyncFactory"),
        SET_NETWORK_TIMEOUT("setNetworkTimeout"),
        DEREGISTER_DRIVER("deregisterDriver");

        private final String permissionName;

        JdbcPermission(String permissionName) {
            this.permissionName = permissionName;
        }

        public String getPermissionName() {
            return permissionName;
        }

        public static JdbcPermission fromName(String name) {
            for (JdbcPermission values : JdbcPermission.values()) {
                if (values.permissionName.equals(name)) {
                    return values;
                }
            }
            return null;
        }
    }

    private static class JdbcSecurityManager extends SecurityManager {
        private final boolean allowAll;
        private final EnumSet<JdbcPermission> exclusions;

        /**
         * Configures a new {@link SecurityManager} that follows the custom rules.
         *
         * @param allowAll whether permissions are allowed by default or not
         * @param exclusions optional set of exclusions
         */
        private JdbcSecurityManager(boolean allowAll, EnumSet<JdbcPermission> exclusions) {
            this.exclusions = exclusions;
            this.allowAll = allowAll;
        }

        @Override
        public void checkPermission(Permission permission) {
            JdbcPermission jdbcPermission = JdbcPermission.fromName(permission.getName());
            if (jdbcPermission == null) {
                return;
            }
            boolean allowed = allowAll ^ exclusions.contains(jdbcPermission);
            if (!allowed) {
                throw new SecurityException("Permission " + jdbcPermission.getPermissionName() + " is not allowed");
            }
        }
    }
}

