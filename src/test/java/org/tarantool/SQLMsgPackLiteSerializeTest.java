package org.tarantool;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;

import org.tarantool.jdbc.SQLMsgPackLite;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.math.BigDecimal;

/**
 * Test set for {@link SQLMsgPackLite#pack(Object, OutputStream)} method.
 */
public class SQLMsgPackLiteSerializeTest {

    private SQLMsgPackLite msgPackLite;
    private ByteArrayOutputStream outputStream;

    @BeforeEach
    void setUp() {
        msgPackLite = new SQLMsgPackLite();
        outputStream = new ByteArrayOutputStream();
    }

    /**
     * java.sql.Date stores a 64-bit big-endian unsigned integer.
     *
     * +--------+--------+
     * |  0xcf  |ZZZZZZZZ| x8
     * +--------+--------+
     *
     * @see MsgPackLiteSerializeTest#testPackUnsignedIntegers()
     */
    @Test
    void testPackDate() throws IOException {
        long timeMillis = 1573826002351L;

        byte[] expectedOutput = {
            // 1573826002351
            (byte) 0xcf,
            (byte) 0x00, (byte) 0x00, (byte) 0x01, (byte) 0x6e, (byte) 0x6f, (byte) 0x56, (byte) 0xfd, (byte) 0xaf
        };

        msgPackLite.pack(new java.sql.Date(timeMillis), outputStream);
        assertArrayEquals(expectedOutput, outputStream.toByteArray());
    }

    /**
     * java.sql.Time stores a 64-bit big-endian unsigned integer.
     *
     * +--------+--------+
     * |  0xcf  |ZZZZZZZZ| x8
     * +--------+--------+
     *
     * @see MsgPackLiteSerializeTest#testPackUnsignedIntegers()
     */
    @Test
    void testPackTime() throws IOException {
        long timeMillis = 1573826660627L;

        byte[] expectedOutput = {
            // 1573826660627
            (byte) 0xcf,
            (byte) 0x00, (byte) 0x00, (byte) 0x01, (byte) 0x6e, (byte) 0x6f, (byte) 0x61, (byte) 0x09, (byte) 0x13
        };

        msgPackLite.pack(new java.sql.Time(timeMillis), outputStream);
        assertArrayEquals(expectedOutput, outputStream.toByteArray());
    }

    /**
     * java.sql.Timestamp stores a 64-bit big-endian unsigned integer.
     *
     * +--------+--------+
     * |  0xcf  |ZZZZZZZZ| x8
     * +--------+--------+
     *
     * @see MsgPackLiteSerializeTest#testPackUnsignedIntegers()
     */
    @Test
    void testPackTimestamp() throws IOException {
        long timeMillis = 1573826790496L;

        byte[] expectedOutput = {
            // 1573826790496
            (byte) 0xcf,
            (byte) 0x00, (byte) 0x00, (byte) 0x01, (byte) 0x6e, (byte) 0x6f, (byte) 0x63, (byte) 0x04, (byte) 0x60
        };

        msgPackLite.pack(new java.sql.Timestamp(timeMillis), outputStream);
        assertArrayEquals(expectedOutput, outputStream.toByteArray());
    }

    /**
     * java.math.BigDecimal stores a byte array (string).
     *
     * @see MsgPackLiteSerializeTest#testPackFixedStrings()
     * @see MsgPackLiteSerializeTest#testPackStrings()
     */
    @Test
    void testPackUnsignedIntegers() throws IOException {
        msgPackLite.pack(new BigDecimal("10000"), outputStream);
        msgPackLite.pack(new BigDecimal("-20000"), outputStream);
        msgPackLite.pack(new BigDecimal("10.5"), outputStream);

        byte[] expected = {
            // "10000"
            (byte) 0xa5, (byte) 0x31, (byte) 0x30, (byte) 0x30, (byte) 0x30, (byte) 0x30,
            // "-20000"
            (byte) 0xa6, (byte) 0x2d, (byte) 0x32, (byte) 0x30, (byte) 0x30, (byte) 0x30, (byte) 0x30,
            // "10.5"
            (byte) 0xa4, (byte) 0x31, (byte) 0x30, (byte) 0x2e, (byte) 0x35
        };
        assertArrayEquals(expected, outputStream.toByteArray());
    }
}
