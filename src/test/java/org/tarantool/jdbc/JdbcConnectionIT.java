package org.tarantool.jdbc;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.tarantool.TestAssumptions.assumeMinimalServerVersion;
import static org.tarantool.TestAssumptions.assumeServerVersionLessThan;

import org.tarantool.TarantoolTestHelper;
import org.tarantool.util.ServerVersion;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;

import java.sql.ClientInfoStatus;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLClientInfoException;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLSyntaxErrorException;
import java.sql.Statement;
import java.util.Map;

public class JdbcConnectionIT {

    private static TarantoolTestHelper testHelper;

    private Connection conn;

    @BeforeAll
    public static void setupEnv() {
        testHelper = new TarantoolTestHelper("jdbc-connection-it");
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
        conn = DriverManager.getConnection(SqlTestUtils.makeDefaultJdbcUrl());
    }

    @AfterEach
    public void tearDownTest() throws SQLException {
        assumeMinimalServerVersion(testHelper.getInstanceVersion(), ServerVersion.V_2_1);
        if (conn != null) {
            conn.close();
        }
    }

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
    }

    @Test
    void testIsValidCheck() throws SQLException {
        assertTrue(conn.isValid(2));
        assertThrows(SQLException.class, () -> conn.isValid(-1));

        conn.close();
        assertFalse(conn.isValid(2));
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
    }

    @Test
    public void testCreateUnsupportedHoldableStatement() throws SQLException {
        assertThrows(
            SQLFeatureNotSupportedException.class,
            () -> conn.createStatement(
                ResultSet.TYPE_FORWARD_ONLY,
                ResultSet.CONCUR_READ_ONLY,
                ResultSet.CLOSE_CURSORS_AT_COMMIT
            ));
    }

    @Test
    public void testCreateWrongHoldableStatement() throws SQLException {
        assertThrows(SQLException.class, () -> {
            conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY, Integer.MAX_VALUE);
        });
        assertThrows(SQLException.class, () -> {
            conn.createStatement(
                ResultSet.TYPE_FORWARD_ONLY,
                ResultSet.CONCUR_READ_ONLY,
                -65
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
    }

    @Test
    public void testPrepareUnsupportedHoldableStatement() throws SQLException {
        assertThrows(SQLFeatureNotSupportedException.class,
            () -> {
                String sqlString = "SELECT * FROM TEST";
                conn.prepareStatement(
                    sqlString,
                    ResultSet.TYPE_FORWARD_ONLY,
                    ResultSet.CONCUR_READ_ONLY,
                    ResultSet.CLOSE_CURSORS_AT_COMMIT
                );
            });
    }

    @Test
    public void testPrepareWrongHoldableStatement() throws SQLException {
        String sqlString = "SELECT * FROM TEST";
        assertThrows(SQLException.class,
            () -> {
                conn.prepareStatement(
                    sqlString,
                    ResultSet.TYPE_FORWARD_ONLY,
                    ResultSet.CONCUR_READ_ONLY,
                    Integer.MAX_VALUE
                );
            });
        assertThrows(SQLException.class,
            () -> {
                conn.prepareStatement(
                    sqlString,
                    ResultSet.TYPE_FORWARD_ONLY,
                    ResultSet.CONCUR_READ_ONLY, -190
                );
            });
    }

    @Test
    public void testCreateScrollableStatement() throws SQLException {
        Statement statement = conn.createStatement();
        assertEquals(ResultSet.TYPE_FORWARD_ONLY, statement.getResultSetType());

        statement = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
        assertEquals(ResultSet.TYPE_FORWARD_ONLY, statement.getResultSetType());

        statement = conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
        assertEquals(ResultSet.TYPE_SCROLL_INSENSITIVE, statement.getResultSetType());

        statement = conn.createStatement(
            ResultSet.TYPE_FORWARD_ONLY,
            ResultSet.CONCUR_READ_ONLY,
            ResultSet.HOLD_CURSORS_OVER_COMMIT
        );
        assertEquals(ResultSet.TYPE_FORWARD_ONLY, statement.getResultSetType());

        statement = conn.createStatement(
            ResultSet.TYPE_SCROLL_INSENSITIVE,
            ResultSet.CONCUR_READ_ONLY,
            ResultSet.HOLD_CURSORS_OVER_COMMIT
        );
        assertEquals(ResultSet.TYPE_SCROLL_INSENSITIVE, statement.getResultSetType());
    }

    @Test
    public void testCreateUnsupportedScrollableStatement() throws SQLException {
        assertThrows(SQLFeatureNotSupportedException.class, () -> {
            conn.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_READ_ONLY);
        });
        assertThrows(SQLFeatureNotSupportedException.class, () -> {
            conn.createStatement(
                ResultSet.TYPE_SCROLL_SENSITIVE,
                ResultSet.CONCUR_READ_ONLY,
                ResultSet.HOLD_CURSORS_OVER_COMMIT
            );
        });
    }

    @Test
    public void testCreateWrongScrollableStatement() {
        assertThrows(SQLException.class, () -> {
            conn.createStatement(Integer.MAX_VALUE, ResultSet.CONCUR_READ_ONLY, ResultSet.HOLD_CURSORS_OVER_COMMIT);
        });
        assertThrows(SQLException.class, () -> {
            conn.createStatement(-47, ResultSet.CONCUR_READ_ONLY, ResultSet.HOLD_CURSORS_OVER_COMMIT);
        });
    }

    @Test
    public void testPrepareScrollableStatement() throws SQLException {
        String sqlString = "TEST";
        Statement statement = conn.prepareStatement(sqlString);
        assertEquals(ResultSet.TYPE_FORWARD_ONLY, statement.getResultSetType());

        statement = conn.prepareStatement(sqlString, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
        assertEquals(ResultSet.TYPE_FORWARD_ONLY, statement.getResultSetType());

        statement = conn.prepareStatement(
            sqlString,
            ResultSet.TYPE_FORWARD_ONLY,
            ResultSet.CONCUR_READ_ONLY,
            ResultSet.HOLD_CURSORS_OVER_COMMIT
        );
        assertEquals(ResultSet.TYPE_FORWARD_ONLY, statement.getResultSetType());
    }

    @Test
    public void testPrepareUnsupportedScrollableStatement() throws SQLException {
        assertThrows(SQLFeatureNotSupportedException.class, () -> {
            String sqlString = "SELECT * FROM TEST";
            conn.prepareStatement(sqlString, ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_READ_ONLY);
        });
        assertThrows(SQLFeatureNotSupportedException.class, () -> {
            String sqlString = "SELECT * FROM TEST";
            conn.prepareStatement(
                sqlString,
                ResultSet.TYPE_SCROLL_SENSITIVE,
                ResultSet.CONCUR_READ_ONLY,
                ResultSet.CLOSE_CURSORS_AT_COMMIT
            );
        });
    }

    @Test
    public void testPrepareWrongScrollableStatement() throws SQLException {
        String sqlString = "SELECT * FROM TEST";
        assertThrows(SQLException.class,
            () -> {
                conn.prepareStatement(
                    sqlString,
                    ResultSet.TYPE_FORWARD_ONLY,
                    ResultSet.CONCUR_READ_ONLY,
                    Integer.MAX_VALUE
                );
            });
        assertThrows(SQLException.class, () -> {
            conn.prepareStatement(sqlString, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY, -90);
        });
    }

    @Test
    public void testCreateConcurrentStatement() throws SQLException {
        Statement statement = conn.createStatement();
        assertEquals(ResultSet.CONCUR_READ_ONLY, statement.getResultSetConcurrency());

        statement = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
        assertEquals(ResultSet.CONCUR_READ_ONLY, statement.getResultSetConcurrency());

        statement = conn.createStatement(
            ResultSet.TYPE_FORWARD_ONLY,
            ResultSet.CONCUR_READ_ONLY,
            ResultSet.HOLD_CURSORS_OVER_COMMIT
        );
        assertEquals(ResultSet.CONCUR_READ_ONLY, statement.getResultSetConcurrency());
    }

    @Test
    public void testCreateUnsupportedConcurrentStatement() throws SQLException {
        assertThrows(SQLFeatureNotSupportedException.class, () -> {
            conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE);
        });
        assertThrows(SQLFeatureNotSupportedException.class,
            () -> {
                conn.createStatement(
                    ResultSet.TYPE_FORWARD_ONLY,
                    ResultSet.CONCUR_UPDATABLE,
                    ResultSet.HOLD_CURSORS_OVER_COMMIT
                );
            });
    }

    @Test
    public void testCreateWrongConcurrentStatement() {
        assertThrows(SQLException.class, () -> {
            conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, Integer.MAX_VALUE, ResultSet.HOLD_CURSORS_OVER_COMMIT);
        });
        assertThrows(SQLException.class, () -> {
            conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, -7213, ResultSet.HOLD_CURSORS_OVER_COMMIT);
        });
    }

    @Test
    public void testCreateStatementWithClosedConnection() {
        assertThrows(SQLException.class,
            () -> {
                conn.close();
                conn.createStatement(
                    ResultSet.TYPE_FORWARD_ONLY,
                    ResultSet.CONCUR_READ_ONLY,
                    ResultSet.HOLD_CURSORS_OVER_COMMIT
                );
            });
        assertThrows(SQLException.class,
            () -> {
                conn.close();
                conn.createStatement(
                    ResultSet.TYPE_SCROLL_INSENSITIVE,
                    ResultSet.CONCUR_READ_ONLY,
                    ResultSet.HOLD_CURSORS_OVER_COMMIT
                );
            });
    }

    @Test
    public void testPrepareStatementWithClosedConnection() {
        String sqlString = "SELECT * FROM TEST";
        assertThrows(SQLException.class,
            () -> {
                conn.close();
                conn.prepareStatement(
                    sqlString,
                    ResultSet.TYPE_FORWARD_ONLY,
                    ResultSet.CONCUR_READ_ONLY,
                    ResultSet.HOLD_CURSORS_OVER_COMMIT
                );
            });
        assertThrows(SQLException.class,
            () -> {
                conn.close();
                conn.prepareStatement(
                    sqlString,
                    ResultSet.TYPE_SCROLL_INSENSITIVE,
                    ResultSet.CONCUR_READ_ONLY,
                    ResultSet.HOLD_CURSORS_OVER_COMMIT
                );
            });
    }

    @Test
    public void testGeneratedKeys() throws SQLException {
        String sql = "SELECT * FROM test";
        PreparedStatement preparedStatement = conn.prepareStatement(sql, Statement.NO_GENERATED_KEYS);
        assertNotNull(preparedStatement);
        preparedStatement.close();

        assertThrows(SQLFeatureNotSupportedException.class, () -> conn.prepareStatement(sql, new int[] { 1 }));
        assertThrows(SQLFeatureNotSupportedException.class, () -> conn.prepareStatement(sql, new String[] { "id" }));

        assertThrows(SQLException.class, () -> conn.prepareStatement(sql, Integer.MAX_VALUE));
        assertThrows(SQLException.class, () -> conn.prepareStatement(sql, Integer.MIN_VALUE));
        assertThrows(SQLException.class, () -> conn.prepareStatement(sql, -76));
    }

    @Test
    void testSetClientInfoProperties() {
        String targetProperty = "ApplicationName";

        SQLClientInfoException exception = assertThrows(
            SQLClientInfoException.class,
            () -> conn.setClientInfo(targetProperty, "TestApp")
        );

        Map<String, ClientInfoStatus> failedProperties = exception.getFailedProperties();
        assertEquals(1, failedProperties.size());
        assertEquals(ClientInfoStatus.REASON_UNKNOWN_PROPERTY, failedProperties.get(targetProperty));
    }

    @Test
    void testLimitEscapeProcessing() throws SQLException {
        String[][] expressions = {
            { "select * from table {limit 10}", "select * from table limit 10" },
            { "select * from table {limit 10 offset 20}", "select * from table limit 10 offset 20" },
            {
                "select * from table where val = 'val {limit 10}' {limit 15}",
                "select * from table where val = 'val {limit 10}' limit 15"
            },
            { "select * from table {limit 10}", "select * from table limit 10" },
            { "select * from table /*{limit 10}*/ {limit 25}", "select * from table /*{limit 10}*/ limit 25" },
            { "select * from table {limit 25} -- {limit 45}", "select * from table limit 25 -- {limit 45}" },
            { "select * from table -- {limit 45}\n{limit 10}", "select * from table -- {limit 45}\nlimit 10" },
            { "select * from table {limit (10) offset (((20)))}", "select * from table limit (10) offset (((20)))" }
        };

        for (String[] pair : expressions) {
            assertEquals(pair[1], conn.nativeSQL(pair[0]));
        }
    }

    @Test
    void testLikeEscapeProcessing() throws SQLException {
        String[][] expressions = {
            {
                "select * from table where val like '|%type' {escape '|'}",
                "select * from table where val like '|%type' escape '|'"
            },
            {
                "select * from table where val like '|%type' -- {escape '|'}",
                "select * from table where val like '|%type' -- {escape '|'}"
            },
            {
                "select * from table where /* use {escape '&'} */ val like '|&type&&' {escape '&'}",
                "select * from table where /* use {escape '&'} */ val like '|&type&&' escape '&'",
            },
            {
                "select * from table where /* use {escape '&'} */ val like '|&type&&' {escape '&'}",
                "select * from table where /* use {escape '&'} */ val like '|&type&&' escape '&'",
            },
            {
                "select * from \"TABLE\" where val like '|&type&&' {escape {fn char(38)}}",
                "select * from \"TABLE\" where val like '|&type&&' escape CHAR(38)",
            }
        };

        for (String[] pair : expressions) {
            assertEquals(pair[1], conn.nativeSQL(pair[0]));
        }
    }

    @Test
    void testOuterJoinEscapeProcessing() throws SQLException {
        String[][] expressions = {
            {
                "select * from {oj table1 left outer join table2 on type = 4} {limit 5}",
                "select * from table1 left outer join table2 on type = 4 limit 5",
            },
            {
                "select * from /* {oj} */ {oj table1 left outer join table2 on type = 4} {limit 5}",
                "select * from /* {oj} */ table1 left outer join table2 on type = 4 limit 5",
            },
            {
                "select * from {oj t1 left outer join (select id from {oj t2 right outer join t3 on 1 = 1}) on id = 4}",
                "select * from t1 left outer join (select id from t2 right outer join t3 on 1 = 1) on id = 4",
            }
        };

        for (String[] pair : expressions) {
            assertEquals(pair[1], conn.nativeSQL(pair[0]));
        }
    }

    @Test
    void testSystemFunctionsEscapeProcessing() throws SQLException {
        String[][] expressions = {
            { "select {fn database()}", "select 'universe'" },
            { "select {fn user()}", "select 'test_admin'" },
            { "select {fn ifnull(null, 'non null string')}", "select IFNULL(null, 'non null string')" },
            { "select {fn ifnull({fn user()}, {fn database()})}", "select IFNULL('test_admin', 'universe')" }
        };

        for (String[] pair : expressions) {
            assertEquals(pair[1], conn.nativeSQL(pair[0]));
        }
    }

    @Test
    void testNumericFunctionsEscapeProcessing() throws SQLException {
        String[][] expressions = {
            { "select {fn abs(-10)}", "select ABS(-10)" },
            { "select {fn pi()}", "select 3.141592653589793" },
            { "select {fn round(-3.14, 1)}", "select ROUND(-3.14, 1)" },
            {
                "select 2 * {fn pi()} * {fn pi()} / {fn abs(4 - {fn round({fn pi()}, 4)})}",
                "select 2 * 3.141592653589793 * 3.141592653589793 / ABS(4 - ROUND(3.141592653589793, 4))"
            }
        };

        for (String[] pair : expressions) {
            assertEquals(pair[1], conn.nativeSQL(pair[0]));
        }
    }

    @Test
    void testStringFunctionsEscapeProcessing() throws SQLException {
        String[][] expressions = {
            { "select {fn char(32)}", "select CHAR(32)" },
            { "select {fn char_length(val)}", "select CHAR_LENGTH(val)" },
            { "select {fn character_length(val)}", "select CHARACTER_LENGTH(val)" },
            { "select {fn concat('abc', '123')}", "select ('abc' || '123')" },
            { "select {fn lcase('aBc')}", "select LOWER('aBc')" },
            { "select {fn left('abcdfgh', 3)}", "select SUBSTR('abcdfgh', 1, 3)" },
            { "select {fn length('value')}", "select LENGTH(TRIM(TRAILING FROM 'value'))" },
            { "select {fn replace('value', 'a', 'o')}", "select REPLACE('value', 'a', 'o')" },
            { "select {fn right('value', 2)}", "select SUBSTR('value', -(2))" },
            { "select {fn soundex('one')}", "select SOUNDEX('one')" },
            { "select {fn substring('value', 2, len)}", "select SUBSTR('value', 2, len)" },
            { "select {fn ucase('value')}", "select UPPER('value')" },
            {
                "select {fn lcase({fn substring({fn concat('value', '12345')}, 1, {fn abs(num)})})}",
                "select LOWER(SUBSTR(('value' || '12345'), 1, ABS(num)))"
            }
        };

        for (String[] pair : expressions) {
            assertEquals(pair[1], conn.nativeSQL(pair[0]));
        }
    }

    @Test
    void testStringFunctionsEscapeProcessingBefore22() throws SQLException {
        assumeServerVersionLessThan(testHelper.getInstanceVersion(), ServerVersion.V_2_2);

        String[][] expressions = {
            { "select {fn ltrim('  value')}", "select LTRIM('  value')" },
            { "select {fn rtrim('value  ')}", "select RTRIM('value  ')" },
        };

        for (String[] pair : expressions) {
            assertEquals(pair[1], conn.nativeSQL(pair[0]));
        }
    }

    @Test
    void testStringFunctionsEscapeProcessingFrom22() throws SQLException {
        assumeMinimalServerVersion(testHelper.getInstanceVersion(), ServerVersion.V_2_2);

        String[][] expressions = {
            { "select {fn ltrim('  value')}", "select TRIM(LEADING FROM '  value')" },
            { "select {fn rtrim('value  ')}", "select TRIM(TRAILING FROM 'value  ')" },
        };

        for (String[] pair : expressions) {
            assertEquals(pair[1], conn.nativeSQL(pair[0]));
        }
    }

    @Test
    void testNoFunctionsEscapeProcessing() throws SQLException {
        String[] expressions = {
            "select * from table /* {fn abs(-10)} */",
            "select * from table",
            "select 1 -- ping",
            "select 3 -- {fn round(3.14, 0)}",
            "select '{fn pi()}'"
        };

        for (String expression : expressions) {
            assertEquals(expression, conn.nativeSQL(expression));
        }
    }

    @Test
    void testEscapeWithExtraWhitespaces() throws SQLException {
        String[][] expressions = {
            { "select {fn database(   )}", "select 'universe'" },
            { "select {    fn user()}", "select 'test_admin'" },
            { "select {fn user()    }", "select 'test_admin'" },
            { "select {    fn user()    }", "select 'test_admin'" },
        };

        for (String[] pair : expressions) {
            assertEquals(pair[1], conn.nativeSQL(pair[0]));
        }
    }

    @Test
    void testEscapeWithComments() throws SQLException {
        String[][] expressions = {
            {
                "select * from {oj table1 left outer join table2 /* join */ on type = 4} {limit 5 /*no more than 5*/}",
                "select * from table1 left outer join table2 /* join */ on type = 4 limit 5 /*no more than 5*/",
            },
            {
                "select {fn ucase(-- string in any case\n'ram')}",
                "select UPPER(-- string in any case\n'ram')" },
            {
                "select {fn round(/* number */ val, /* places */ 3)}",
                "select ROUND(/* number */ val, /* places */ 3)"
            },
            {
                "select {fn database(/* get db name */)}",
                "select 'universe'"
            },
            {
                "select {fn database(-- get db name\n)}",
                "select 'universe'"
            },
            {
                "select {fn soundex(/* 12 */ 'apple')}",
                "select SOUNDEX(/* 12 */ 'apple')",
            },
            {
                "select {fn lcase /* to lower case */ ('ORaNGE')}",
                "select LOWER('ORaNGE')",
            },
            {
                "select {fn /* get char */ char(32) /* end */}",
                "select CHAR(32)",
            },
            {
                "select {fn concat(/*first*/'abc', /*second*/'def')}",
                "select (/*first*/'abc' || /*second*/'def')",
            },
            {
                "select /* 2 * pi * abs(round(-6, 0)) */ 2 * {fn pi(/*3.14*/)} * " +
                    "{fn abs(/*abs*/{fn round(/*todo*/-6, /*check*/0)})}",
                "select /* 2 * pi * abs(round(-6, 0)) */ 2 * 3.141592653589793 * " +
                    "ABS(/*abs*/ROUND(/*todo*/-6, /*check*/0))"
            },
            {
                "select * FROM test /* limit rows */ {limit 10 /* ten should be enough */}",
                "select * FROM test /* limit rows */ limit 10 /* ten should be enough */",
            },
        };

        for (String[] pair : expressions) {
            assertEquals(pair[1], conn.nativeSQL(pair[0]));
        }
    }

    @Test
    void testWrongFunctionsEscapeProcessing() throws SQLException {
        String[] expressions = {
            "select {fn char(48)", // open escape expression
            "select /* {fn char_length(val)}", // open block comment
            "select {fn character_length('asd)}", // open string literal
            "select }fn concat('abc', '123')}", // bad '}'
            "select {fn lcase('aBc')}}", // extra }
            "select * from \"TABLE where val = {fn left('abcdfgh', 3)}", // open quoted identifier
            "select {fn ('value')}", // missed function name
            "select {fn ltrim(('  value')}", // extra (
            "select {fn 0replace('value', 'a', 'o')}", // wrong identifier
            "select {fn right_part('value', 2)}", // unsupported/unknown function name
            "select {comment 'your comment here'}", // unsupported escape syntax
            "select {fn soundex('one', 3)}", // unsupported function signature (2 args)
            "select {fn soundex('one')2'string' }", // extra non-blank symbols after a function declaration
            "select {fn ucase}", // missed function braces
            "select {fn substring('abc', 1, )}", // missed last function braces
            "select {fn substring(, 1, 2)}", // missed first function braces
        };

        for (String badExpression : expressions) {
            assertThrows(SQLSyntaxErrorException.class, () -> conn.nativeSQL(badExpression));
        }
    }

}
