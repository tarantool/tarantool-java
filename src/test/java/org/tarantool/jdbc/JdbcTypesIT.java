package org.tarantool.jdbc;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.tarantool.TestUtils.fromHex;
import static org.tarantool.jdbc.SqlTestUtils.getInsertSQL;
import static org.tarantool.jdbc.SqlTestUtils.getParameterizedInsertSQL;
import static org.tarantool.jdbc.SqlTestUtils.getSelectSQL;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.concurrent.atomic.AtomicInteger;

public class JdbcTypesIT extends AbstractJdbcIT {
    private static AtomicInteger KEY_CNTR = new AtomicInteger(1);

    public static Integer[] INT_VALS = new Integer[] { Integer.MIN_VALUE, 0, Integer.MAX_VALUE, 1, 100, -50 };
    public static Double[] DOUBLE_VALS = new Double[] { 0.0d, Double.MIN_VALUE, Double.MAX_VALUE, 1.00001d, 100.5d };
    public static Float[] FLOAT_VALS = new Float[] { 0.0f, Float.MIN_VALUE, Float.MAX_VALUE, 1.00001f, 100.5f };
    public static String[] STRING_VALS = new String[] { "", "1", "A", "test test" };
    public static Byte[] BYTE_VALS = new Byte[] { Byte.MIN_VALUE, Byte.MAX_VALUE, (byte)0, (byte)100 };
    public static Short[] SHORT_VALS = new Short[] { Short.MIN_VALUE, Short.MAX_VALUE, (short)0, (short)1000 };
    public static Long[] LONG_VALS = new Long[] { Long.MIN_VALUE, Long.MAX_VALUE, 0L, 100000000L};
    public static BigDecimal[] BIGDEC_VALS = new BigDecimal[] { BigDecimal.valueOf(Double.MIN_VALUE),
        BigDecimal.valueOf(Double.MAX_VALUE), BigDecimal.ZERO, BigDecimal.ONE };
    public static byte[][] BINARY_VALS = new byte[][] { fromHex(""), fromHex("00"), fromHex("FFFF"),
        fromHex("0102030405060708") };
    public static Date[] DATE_VALS = new Date[] { Date.valueOf("1983-03-14"), new Date(129479994) };

    public <T> TestHelper<T> makeHelper(Class<T> cls) {
        return new TestHelper<T>(cls);
    }

    class TestHelper<T> {
        Class<T> cls;
        T[] vals;
        TntSqlType[] colTypes;
        int start;

        TestHelper(Class<T> cls) {
            this.cls = cls;
        }

        TestHelper<T> setValues(T... vals) {
            this.vals = vals;
            start = KEY_CNTR.getAndAdd(vals.length);
            return this;
        }

        TestHelper<T> setColumns(TntSqlType... colTypes) {
            this.colTypes = colTypes;
            return this;
        }

        void testSetParameter() throws SQLException {
            checkSetParameter();
            checkResultSetGet();
        }

        void testGetColumn() throws SQLException {
            sqlExec(getInsertSQL("test_types", start, colTypes, vals));
            checkResultSetGet();
        }

        private void checkSetParameter() throws SQLException {
            String sql = getParameterizedInsertSQL("test_types", colTypes);
            PreparedStatement prep = conn.prepareStatement(sql);
            try {
                assertNotNull(prep);
                int count = start;
                for (T val : vals) {
                    prep.setInt(1, count++);
                    for (int col = 0; col < colTypes.length; col++) {
                        apply(prep, 2 + col, val);
                    }
                    assertEquals(1, prep.executeUpdate());
                }
            } catch (Throwable e) {
                throw new SQLException(e);
            } finally {
                prep.close();
            }
        }

        private void checkResultSetGet() throws SQLException {
            Statement stmt = conn.createStatement();
            try {
                String sql = getSelectSQL("test_types", start, start + vals.length, colTypes);
                ResultSet rs = stmt.executeQuery(sql);
                assertNotNull(rs);
                try {
                    for (int row = 0; row < vals.length; row++) {
                        assertTrue(rs.next());
                        int valIdx = rs.getInt(1) - start;
                        T val = vals[valIdx];
                        for (int col = 0; col < colTypes.length; col++) {
                            TntSqlType tntSqlType = colTypes[col];
                            check(rs, 2 + col, "F" + tntSqlType.ordinal(), val);
                        }
                    }
                } finally {
                    rs.close();
                }
            } finally {
                stmt.close();
            }
        }

        protected void apply(PreparedStatement ps, int col, T val) throws SQLException {
            if (cls == Byte.class) {
                ps.setByte(col, (Byte)val);
            } else if (cls == Short.class) {
                ps.setShort(col, (Short)val);
            } else if (cls == Integer.class) {
                ps.setInt(col, (Integer)val);
            } else if (cls == Long.class) {
                ps.setLong(col, (Long)val);
            } else if (cls == String.class) {
                ps.setString(col, (String)val);
            } else if (cls == Float.class) {
                ps.setFloat(col, (Float)val);
            } else if (cls == Double.class) {
                ps.setDouble(col, (Double)val);
            } else if (cls == Boolean.class) {
                ps.setBoolean(col, (Boolean)val);
            } else if (cls == BigDecimal.class) {
                ps.setBigDecimal(col, (BigDecimal) val);
            } else if (cls == byte[].class) {
                ps.setBytes(col, (byte[])val);
            } else if (cls == Date.class) {
                ps.setDate(col, (Date)val);
            } else if (cls == Time.class) {
                ps.setTime(col, (Time)val);
            } else if (cls == Timestamp.class) {
                ps.setTimestamp(col, (Timestamp)val);
            } else {
                throw new IllegalArgumentException("val is of unexpected type " + cls.getName());
            }
        }

        protected void check(ResultSet rs, int col, String name, T val) throws SQLException {
            if (cls == Byte.class) {
                assertEquals(val, rs.getByte(col));
                assertEquals(val, rs.getByte(name));
            } else if (cls == Short.class) {
                assertEquals(val, rs.getShort(col));
                assertEquals(val, rs.getShort(name));
            } else if (cls == Integer.class) {
                assertEquals(val, rs.getInt(col));
                assertEquals(val, rs.getInt(name));
            } else if (cls == Long.class) {
                assertEquals(val, rs.getLong(col));
                assertEquals(val, rs.getLong(name));
            } else if (cls == String.class) {
                assertEquals(val, rs.getString(col));
                assertEquals(val, rs.getString(name));
            } else if (cls == Float.class) {
                assertEquals((Float)val, rs.getFloat(col), Math.ulp(1.0f));
                assertEquals((Float)val, rs.getFloat(name), Math.ulp(1.0f));
            } else if (cls == Double.class) {
                assertEquals((Double)val, rs.getDouble(col), Math.ulp(1.0d));
                assertEquals((Double)val, rs.getDouble(name), Math.ulp(1.0d));
            } else if (cls == Boolean.class) {
                assertEquals(val, rs.getBoolean(col));
                assertEquals(val, rs.getBoolean(name));
            } else if (cls == BigDecimal.class) {
                assertEquals(0, ((BigDecimal)val).compareTo(rs.getBigDecimal(col)));
                assertEquals(0, ((BigDecimal)val).compareTo(rs.getBigDecimal(name)));
            } else if (cls == byte[].class) {
                assertArrayEquals((byte[])val, rs.getBytes(col));
                assertArrayEquals((byte[])val, rs.getBytes(name));
            } else if (cls == Date.class) {
                assertEquals(val, rs.getDate(col));
                assertEquals(val, rs.getDate(name));
            } else if (cls == Time.class) {
                assertEquals(val, rs.getTime(col));
                assertEquals(val, rs.getTime(name));
            } else if (cls == Timestamp.class) {
                assertEquals(val, rs.getTimestamp(col));
                assertEquals(val, rs.getTimestamp(name));
            } else {
                throw new IllegalArgumentException("val is of unexpected type " + cls.getName());
            }
        }
    }
}
