package org.tarantool.jdbc;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import org.tarantool.TarantoolConnection;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;

import java.lang.reflect.Field;
import java.net.Socket;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Statement;

@SuppressWarnings("Since15")
public class JdbcConnectionIT extends AbstractJdbcIT {

    @Test
    public void testCreateStatement() throws SQLException {
        Statement stmt = conn.createStatement();
        assertNotNull(stmt);
        stmt.close();
    }

    @Test
    public void testPrepareStatement() throws SQLException {
        PreparedStatement prep = conn.prepareStatement("INSERT INTO test(id, val) VALUES(?, ?)");
        assertNotNull(prep);
        prep.close();
    }

    @Test
    public void testCloseIsClosed() throws SQLException {
        assertFalse(conn.isClosed());
        conn.close();
        assertTrue(conn.isClosed());
        conn.close();
    }

    @Test
    public void testGetMetaData() throws SQLException {
        DatabaseMetaData meta = conn.getMetaData();
        assertNotNull(meta);
    }

    @Test
    public void testGetSetNetworkTimeout() throws Exception {
        assertEquals(0, conn.getNetworkTimeout());

        SQLException e = assertThrows(SQLException.class, new Executable() {
            @Override
            public void execute() throws Throwable {
                conn.setNetworkTimeout(null, -1);
            }
        });
        assertEquals("Network timeout cannot be negative.", e.getMessage());

        conn.setNetworkTimeout(null, 3000);

        assertEquals(3000, conn.getNetworkTimeout());

        // Check that timeout gets propagated to the socket.
        Field tntCon = SQLConnection.class.getDeclaredField("connection");
        tntCon.setAccessible(true);

        Field sock = TarantoolConnection.class.getDeclaredField("socket");
        sock.setAccessible(true);

        assertEquals(3000, ((Socket) sock.get(tntCon.get(conn))).getSoTimeout());
    }

    @Test
    public void testClosedConnection() throws SQLException {
        conn.close();

        int i = 0;
        for (; i < 5; i++) {
            final int step = i;
            SQLException e = assertThrows(SQLException.class, new Executable() {
                @Override
                public void execute() throws Throwable {
                    switch (step) {
                        case 0:
                            conn.createStatement();
                            break;
                        case 1:
                            conn.prepareStatement("TEST");
                            break;
                        case 2:
                            conn.getMetaData();
                            break;
                        case 3:
                            conn.getNetworkTimeout();
                            break;
                        case 4:
                            conn.setNetworkTimeout(null, 1000);
                            break;
                        default:
                            fail();
                    }
                }
            });
            assertEquals("Connection is closed.", e.getMessage());
        }
        assertEquals(5, i);
    }

    @Test
    void testIsValidCheck() throws SQLException {
        assertTrue(conn.isValid(2000));
        assertThrows(SQLException.class, () -> conn.isValid(-1000));

        conn.close();
        assertFalse(conn.isValid(2000));
    }

    @Test
    public void testConnectionUnwrap() throws SQLException {
        assertEquals(conn, conn.unwrap(SQLConnection.class));
        assertThrows(SQLException.class, () -> conn.unwrap(Integer.class));
    }

    @Test
    public void testConnectionIsWrapperFor() throws SQLException {
        assertTrue(conn.isWrapperFor(SQLConnection.class));
        assertFalse(conn.isWrapperFor(Integer.class));
    }

    @Test
    public void testDefaultGetHoldability() throws SQLException {
        // default connection holdability should be equal to metadata one
        assertEquals(conn.getMetaData().getResultSetHoldability(), conn.getHoldability());
    }

    @Test
    public void testSetAndGetHoldability() throws SQLException {
        conn.setHoldability(ResultSet.HOLD_CURSORS_OVER_COMMIT);
        assertEquals(ResultSet.HOLD_CURSORS_OVER_COMMIT, conn.getHoldability());

        assertThrows(
            SQLFeatureNotSupportedException.class,
            () -> conn.setHoldability(ResultSet.CLOSE_CURSORS_AT_COMMIT)
        );
        assertThrows(SQLException.class, () -> conn.setHoldability(Integer.MAX_VALUE));

        assertThrows(SQLException.class, () -> {
            conn.close();
            conn.setHoldability(ResultSet.HOLD_CURSORS_OVER_COMMIT);
        });
    }

    @Test
    public void testCreateHoldableStatement() throws SQLException {
        Statement statement = conn.createStatement();
        assertEquals(ResultSet.HOLD_CURSORS_OVER_COMMIT, statement.getResultSetHoldability());

        statement = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
        assertEquals(ResultSet.HOLD_CURSORS_OVER_COMMIT, statement.getResultSetHoldability());

        statement = conn.createStatement(
            ResultSet.TYPE_FORWARD_ONLY,
            ResultSet.CONCUR_READ_ONLY,
            ResultSet.HOLD_CURSORS_OVER_COMMIT
        );
        assertEquals(ResultSet.HOLD_CURSORS_OVER_COMMIT, statement.getResultSetHoldability());

        assertThrows(SQLException.class, () -> {
            conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY, Integer.MAX_VALUE);
        });
        assertThrows(
            SQLFeatureNotSupportedException.class,
            () -> conn.createStatement(
                ResultSet.TYPE_FORWARD_ONLY,
                ResultSet.CONCUR_READ_ONLY,
                ResultSet.CLOSE_CURSORS_AT_COMMIT
            ));
        assertThrows(SQLException.class, () -> {
            conn.close();
            conn.createStatement(
                ResultSet.TYPE_FORWARD_ONLY,
                ResultSet.CONCUR_READ_ONLY,
                ResultSet.HOLD_CURSORS_OVER_COMMIT
            );
        });
    }

    @Test
    public void testPrepareHoldableStatement() throws SQLException {
        String sqlString = "TEST";
        Statement statement = conn.prepareStatement(sqlString);
        assertEquals(ResultSet.HOLD_CURSORS_OVER_COMMIT, statement.getResultSetHoldability());

        statement = conn.prepareStatement(sqlString, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
        assertEquals(ResultSet.HOLD_CURSORS_OVER_COMMIT, statement.getResultSetHoldability());

        statement = conn.prepareStatement(
            sqlString,
            ResultSet.TYPE_FORWARD_ONLY,
            ResultSet.CONCUR_READ_ONLY,
            ResultSet.HOLD_CURSORS_OVER_COMMIT
        );
        assertEquals(ResultSet.HOLD_CURSORS_OVER_COMMIT, statement.getResultSetHoldability());

        assertThrows(
            SQLException.class,
            () -> conn.prepareStatement(
                sqlString,
                ResultSet.TYPE_FORWARD_ONLY,
                ResultSet.CONCUR_READ_ONLY,
                Integer.MAX_VALUE
            ));
        assertThrows(
            SQLFeatureNotSupportedException.class,
            () -> conn.prepareStatement(
                sqlString,
                ResultSet.TYPE_FORWARD_ONLY,
                ResultSet.CONCUR_READ_ONLY,
                ResultSet.CLOSE_CURSORS_AT_COMMIT
            ));
        assertThrows(
            SQLException.class,
            () -> {
                conn.close();
                conn.prepareStatement(
                    sqlString,
                    ResultSet.TYPE_FORWARD_ONLY,
                    ResultSet.CONCUR_READ_ONLY,
                    ResultSet.HOLD_CURSORS_OVER_COMMIT
                );
            });
    }

}
