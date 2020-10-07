package org.tarantool;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.tarantool.TestAssertions.assertSubArrayEquals;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;

/**
 * Test set for {@link MsgPackLite#pack(Object, OutputStream)} method.
 */
public class MsgPackLiteSerializeTest {

    private MsgPackLite msgPackLite;
    private ByteArrayOutputStream outputStream;

    @BeforeEach
    void setUp() {
        msgPackLite = new MsgPackLite();
        outputStream = new ByteArrayOutputStream();
    }

    /**
     * Nil format stores nil in 1 byte.
     *
     * nil:
     * +--------+
     * |  0xc0  |
     * +--------+
     *
     * @throws IOException if any IO errors occur
     */
    @Test
    void testPackNull() throws IOException {
        byte[] expectedOutput = { (byte) 0xc0 };

        msgPackLite.pack(null, outputStream);
        assertArrayEquals(expectedOutput, outputStream.toByteArray());
    }

    /**
     * Bool format family stores false or true in 1 byte.
     *
     * false:
     * +--------+
     * |  0xc2  |
     * +--------+
     *
     * true:
     * +--------+
     * |  0xc3  |
     * +--------+
     */
    @Test
    void testPackBoolean() throws IOException {
        byte[] expectedOutput = { (byte) 0xc2, (byte) 0xc3 };

        msgPackLite.pack(false, outputStream);
        msgPackLite.pack(true, outputStream);
        assertArrayEquals(expectedOutput, outputStream.toByteArray());
        outputStream.reset();

        msgPackLite.pack(Boolean.FALSE, outputStream);
        msgPackLite.pack(Boolean.TRUE, outputStream);
        assertArrayEquals(expectedOutput, outputStream.toByteArray());
        outputStream.reset();
    }

    /**
     * positive fixnum stores 7-bit positive integer.
     *
     * +--------+
     * |0XXXXXXX|
     * +--------+
     *
     * 0XXXXXXX is 8-bit unsigned integer [0..127]
     */
    @Test
    void testPackFixedPositiveInteger() throws IOException {
        msgPackLite.pack((byte) 0, outputStream);
        msgPackLite.pack(Byte.valueOf("2"), outputStream);
        msgPackLite.pack((short) 4, outputStream);
        msgPackLite.pack(Short.valueOf("16"), outputStream);
        msgPackLite.pack(32, outputStream);
        msgPackLite.pack(Integer.valueOf("48"), outputStream);
        msgPackLite.pack(64L, outputStream);
        msgPackLite.pack(Long.valueOf("96"), outputStream);
        msgPackLite.pack(BigInteger.valueOf(127), outputStream);

        byte[] expected = {
            (byte) 0x0, (byte) 0x02,
            (byte) 0x04, (byte) 0x10,
            (byte) 0x20, (byte) 0x30,
            (byte) 0x40, (byte) 0x60,
            (byte) 0x7f,
        };

        assertArrayEquals(expected, outputStream.toByteArray());
    }

    /**
     * negative fixnum stores 5-bit negative integer.
     *
     * +--------+
     * |111YYYYY|
     * +--------+
     *
     * 111YYYYY is 8-bit signed integer [-1..-32]
     */
    @Test
    void testPackFixedNegativeInteger() throws IOException {
        msgPackLite.pack(-32, outputStream);
        msgPackLite.pack(-16, outputStream);
        msgPackLite.pack(-1, outputStream);
        byte[] expected = {
            (byte) 0xe0,
            (byte) 0xf0,
            (byte) 0xff
        };
        assertArrayEquals(expected, outputStream.toByteArray());
    }

    /**
     * uint 8 stores a 8-bit unsigned integer.
     *
     * +--------+--------+
     * |  0xcc  |ZZZZZZZZ|
     * +--------+--------+
     *
     * uint 16 stores a 16-bit big-endian unsigned integer
     * +--------+--------+
     * |  0xcd  |ZZZZZZZZ| x2
     * +--------+--------+
     *
     * uint 32 stores a 32-bit big-endian unsigned integer
     * +--------+--------+
     * |  0xce  |ZZZZZZZZ| x4
     * +--------+--------+
     *
     * uint 64 stores a 64-bit big-endian unsigned integer
     * +--------+--------+
     * |  0xcf  |ZZZZZZZZ| x8
     * +--------+--------+
     */
    @Test
    void testPackUnsignedIntegers() throws IOException {
        msgPackLite.pack(255, outputStream);
        msgPackLite.pack(256, outputStream);
        msgPackLite.pack(65535, outputStream);
        msgPackLite.pack(65536, outputStream);
        msgPackLite.pack(4294967295L, outputStream);
        msgPackLite.pack(4294967296L, outputStream);
        msgPackLite.pack(new BigInteger("18446744073709551615"), outputStream);

        byte[] expected = {
            // 255 (2^8-1)
            (byte) 0xcc, (byte) 0xff,
            // 256 (2^8)
            (byte) 0xcd, (byte) 0x01, (byte) 0x00,
            // 65535 (2^16-1)
            (byte) 0xcd, (byte) 0xff, (byte) 0xff,
            // 65536 (2^16)
            (byte) 0xce, (byte) 0x00, (byte) 0x01, (byte) 0x00, (byte) 0x00,
            // 4294967295 (2^32-1)
            (byte) 0xce, (byte) 0xff, (byte) 0xff, (byte) 0xff, (byte) 0xff,
            // 4294967296 (2^32)
            (byte) 0xcf, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x01,
            (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00,
            // 18446744073709551615 (2^64-1)
            (byte) 0xcf, (byte) 0xff, (byte) 0xff, (byte) 0xff, (byte) 0xff,
            (byte) 0xff, (byte) 0xff, (byte) 0xff, (byte) 0xff
        };
        assertArrayEquals(expected, outputStream.toByteArray());
    }

    /**
     * int 8 stores a 8-bit signed integer.
     *
     * +--------+--------+
     * |  0xd0  |ZZZZZZZZ|
     * +--------+--------+
     *
     * int 16 stores a 16-bit big-endian signed integer
     * +--------+--------+
     * |  0xd1  |ZZZZZZZZ| 2x
     * +--------+--------+
     *
     * int 32 stores a 32-bit big-endian signed integer
     * +--------+--------+
     * |  0xd2  |ZZZZZZZZ| 4x
     * +--------+--------+
     *
     * int 64 stores a 64-bit big-endian signed integer
     * +--------+--------+
     * |  0xd3  |ZZZZZZZZ| 8x
     * +--------+--------+
     */
    @Test
    void testPackSignedIntegers() throws IOException {
        msgPackLite.pack(-128, outputStream);
        msgPackLite.pack(-129, outputStream);
        msgPackLite.pack(-32768, outputStream);
        msgPackLite.pack(-32769, outputStream);
        msgPackLite.pack(-2147483648, outputStream);
        msgPackLite.pack(-2147483649L, outputStream);
        msgPackLite.pack(-9223372036854775808L, outputStream);

        byte[] expected = {
            // -128 (-2^7)
            (byte) 0xd0, (byte) 0x80,
            // -129 (-2^7-1)
            (byte) 0xd1, (byte) 0xff, (byte) 0x7f,
            // -32768 (-2^15)
            (byte) 0xd1, (byte) 0x80, (byte) 0x00,
            // -32769 (-2^15-1)
            (byte) 0xd2, (byte) 0xff, (byte) 0xff, (byte) 0x7f, (byte) 0xff,
            // -2147483649 (-2^31)
            (byte) 0xd2, (byte) 0x80, (byte) 0x00, (byte) 0x00, (byte) 0x00,
            // -2147483649 (-2^31-1)
            (byte) 0xd3, (byte) 0xff, (byte) 0xff, (byte) 0xff, (byte) 0xff,
            (byte) 0x7f, (byte) 0xff, (byte) 0xff, (byte) 0xff,
            // -9223372036854775808 (-2^63)
            (byte) 0xd3, (byte) 0x80, (byte) 0x00, (byte) 0x00, (byte) 0x00,
            (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00,
        };
        assertArrayEquals(expected, outputStream.toByteArray());
    }

    /**
     * {@link org.tarantool.Code} stores a unsigned int family.
     */
    @Test
    void testPackCodeClass() throws IOException {
        msgPackLite.pack(Code.SELECT, outputStream);
        msgPackLite.pack(Code.DELETE, outputStream);
        msgPackLite.pack(Code.CALL, outputStream);
        msgPackLite.pack(Code.EXECUTE, outputStream);

        byte[] expectedPositive = {
            // SELECT
            (byte) 0x01,
            // DELETE
            (byte) 0x05,
            // CALL
            (byte) 0x0a,
            // EXECUTE
            (byte) 0x0b
        };
        assertArrayEquals(expectedPositive, outputStream.toByteArray());
    }

    /**
     * float 32 stores a floating point number in IEEE 754
     * single precision floating point number format.
     *
     * +--------+--------+
     * |  0xca  |XXXXXXXX| 4x
     * +--------+--------+
     */
    @Test
    void testPackFloatNumber() throws IOException {
        msgPackLite.pack(Float.NaN, outputStream);
        msgPackLite.pack(Float.NEGATIVE_INFINITY, outputStream);
        msgPackLite.pack(Float.POSITIVE_INFINITY, outputStream);
        msgPackLite.pack(60.5f, outputStream);
        msgPackLite.pack(-100.0f, outputStream);

        byte[] expected = {
            // NaN
            (byte) 0xca, (byte) 0x7f, (byte) 0xc0, (byte) 0x00, (byte) 0x00,
            // -Inf
            (byte) 0xca, (byte) 0xff, (byte) 0x80, (byte) 0x00, (byte) 0x00,
            // Inf
            (byte) 0xca, (byte) 0x7f, (byte) 0x80, (byte) 0x00, (byte) 0x00,
            // 60.5
            (byte) 0xca, (byte) 0x42, (byte) 0x72, (byte) 0x00, (byte) 0x00,
            // -100
            (byte) 0xca, (byte) 0xc2, (byte) 0xc8, (byte) 0x00, (byte) 0x00,
        };
        assertArrayEquals(expected, outputStream.toByteArray());
    }

    /**
     * float 64 stores a floating point number in IEEE 754
     * double precision floating point number format.
     *
     * +--------+--------+
     * |  0xcb  |YYYYYYYY| 8x
     * +--------+--------+
     */
    @Test
    void testPackDoubleNumber() throws IOException {
        msgPackLite.pack(Double.NaN, outputStream);
        msgPackLite.pack(Double.NEGATIVE_INFINITY, outputStream);
        msgPackLite.pack(Double.POSITIVE_INFINITY, outputStream);
        msgPackLite.pack(-213.125d, outputStream);
        msgPackLite.pack(100.5d, outputStream);

        byte[] expected = {
            // NaN
            (byte) 0xcb, (byte) 0x7f, (byte) 0xf8, (byte) 0x00, (byte) 0x00,
            (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00,
            // -Inf
            (byte) 0xcb, (byte) 0xff, (byte) 0xf0, (byte) 0x00, (byte) 0x00,
            (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00,
            // Inf
            (byte) 0xcb, (byte) 0x7f, (byte) 0xf0, (byte) 0x00, (byte) 0x00,
            (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00,
            // -213.125
            (byte) 0xcb, (byte) 0xc0, (byte) 0x6a, (byte) 0xa4, (byte) 0x00,
            (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00,
            // 100.5
            (byte) 0xcb, (byte) 0x40, (byte) 0x59, (byte) 0x20, (byte) 0x00,
            (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00,
        };
        assertArrayEquals(expected, outputStream.toByteArray());
    }

    /**
     * Fixstr stores a byte array whose length is upto 31 bytes.
     *
     * +--------+========+
     * |101XXXXX|  data  |
     * +--------+========+
     */
    @Test
    void testPackFixedStrings() throws IOException {
        msgPackLite.pack("a", outputStream);
        msgPackLite.pack("hello", outputStream);
        msgPackLite.pack("привет", outputStream);

        byte[] expected = {
            // latin "a"
            (byte) 0xa1, (byte) 0x61,
            // latin "hello"
            (byte) 0xa5, (byte) 0x68, (byte) 0x65, (byte) 0x6c, (byte) 0x6c, (byte) 0x6f,
            // cyrillic "привет" (12 bytes in utf-8)
            (byte) 0xac, (byte) 0xd0, (byte) 0xbf, (byte) 0xd1, (byte) 0x80, (byte) 0xd0, (byte) 0xb8, (byte) 0xd0,
            (byte) 0xb2, (byte) 0xd0, (byte) 0xb5, (byte) 0xd1, (byte) 0x82,
        };
        assertArrayEquals(expected, outputStream.toByteArray());
    }

    /**
     * Str 8 stores a byte array whose length is upto (2^8)-1 bytes.
     *
     * +--------+--------+========+
     * |  0xd9  |YYYYYYYY|  data  |
     * +--------+--------+========+
     *
     * Str 16 stores a byte array whose length is upto (2^16)-1 bytes.
     *
     * +--------+--------+--------+========+
     * |  0xda  |ZZZZZZZZ|ZZZZZZZZ|  data  |
     * +--------+--------+--------+========+
     *
     * Str 32 stores a byte array whose length is upto (2^32)-1 bytes.
     *
     * +--------+--------+--------+--------+--------+========+
     * |  0xdb  |AAAAAAAA|AAAAAAAA|AAAAAAAA|AAAAAAAA|  data  |
     * +--------+--------+--------+--------+--------+========+
     */
    @Test
    void testPackStrings() throws IOException {
        byte[] oneByteLength = new byte[202];
        Arrays.fill(oneByteLength, (byte) 'a');
        oneByteLength[0] = (byte) 0xd9;
        oneByteLength[1] = (byte) 0xc8;

        byte[] twoBytesLength = new byte[30003];
        Arrays.fill(twoBytesLength, (byte) 'b');
        twoBytesLength[0] = (byte) 0xda;
        twoBytesLength[1] = (byte) 0x75;
        twoBytesLength[2] = (byte) 0x30;

        byte[] fourBytesLength = new byte[80005];
        Arrays.fill(fourBytesLength, (byte) 'c');
        fourBytesLength[0] = (byte) 0xdb;
        fourBytesLength[1] = (byte) 0x00;
        fourBytesLength[2] = (byte) 0x01;
        fourBytesLength[3] = (byte) 0x38;
        fourBytesLength[4] = (byte) 0x80;

        msgPackLite.pack(new String(oneByteLength, 2, 200, StandardCharsets.UTF_8), outputStream);
        msgPackLite.pack(new String(twoBytesLength, 3, 30000, StandardCharsets.UTF_8), outputStream);
        msgPackLite.pack(new String(fourBytesLength, 5, 80000, StandardCharsets.UTF_8), outputStream);

        byte[] bytes = outputStream.toByteArray();
        assertSubArrayEquals(
            oneByteLength, 0, bytes, 0, 202, () -> "1 byte length string corrupted"
        );
        assertSubArrayEquals(
            twoBytesLength, 0, bytes, 202, 30003, () -> "2 bytes length string corrupted"
        );
        assertSubArrayEquals(
            fourBytesLength, 0, bytes, 202 + 30003, 80005, () -> "4 bytes length string corrupted"
        );
    }

    /**
     * Bin 8 stores a byte array whose length is upto (2^8)-1 bytes.
     *
     * +--------+--------+========+
     * |  0xc4  |XXXXXXXX|  data  |
     * +--------+--------+========+
     *
     * Bin 16 stores a byte array whose length is upto (2^16)-1 bytes.
     *
     * +--------+--------+--------+========+
     * |  0xc5  |YYYYYYYY|YYYYYYYY|  data  |
     * +--------+--------+--------+========+
     *
     * Bin 32 stores a byte array whose length is upto (2^32)-1 bytes.
     *
     * +--------+--------+--------+--------+--------+========+
     * |  0xc6  |ZZZZZZZZ|ZZZZZZZZ|ZZZZZZZZ|ZZZZZZZZ|  data  |
     * +--------+--------+--------+--------+--------+========+
     */
    @Test
    void testPackBinArrays() throws IOException {
        byte[] oneByteLength = new byte[202];
        oneByteLength[0] = (byte) 0xc4;
        oneByteLength[1] = (byte) 0xc8;
        byte b = 0;
        for (int i = 2; i < 202; i++) {
            oneByteLength[i] = b++;
        }

        byte[] twoBytesLength = new byte[30003];
        twoBytesLength[0] = (byte) 0xc5;
        twoBytesLength[1] = (byte) 0x75;
        twoBytesLength[2] = (byte) 0x30;
        b = 0;
        for (int i = 3; i < 30003; i++) {
            twoBytesLength[i] = b++;
        }

        byte[] fourBytesLength = new byte[80005];
        fourBytesLength[0] = (byte) 0xc6;
        fourBytesLength[1] = (byte) 0x00;
        fourBytesLength[2] = (byte) 0x01;
        fourBytesLength[3] = (byte) 0x38;
        fourBytesLength[4] = (byte) 0x80;
        b = 0;
        for (int i = 5; i < 80005; i++) {
            fourBytesLength[i] = b++;
        }

        byte[] firstBinary = new byte[200];
        System.arraycopy(oneByteLength, 2, firstBinary, 0, 200);
        byte[] secondBinary = new byte[30000];
        System.arraycopy(twoBytesLength, 3, secondBinary, 0, 30000);
        byte[] thirdBinary = new byte[80000];
        System.arraycopy(fourBytesLength, 5, thirdBinary, 0, 80000);
        msgPackLite.pack(firstBinary, outputStream);
        msgPackLite.pack(secondBinary, outputStream);
        msgPackLite.pack(thirdBinary, outputStream);

        byte[] bytes = outputStream.toByteArray();
        assertSubArrayEquals(
            oneByteLength, 0, bytes, 0, 202, () -> "1 byte length binary corrupted"
        );
        assertSubArrayEquals(
            twoBytesLength, 0, bytes, 202, 30003, () -> "2 bytes length binary corrupted"
        );
        assertSubArrayEquals(
            fourBytesLength, 0, bytes, 202 + 30003, 80005, () -> "4 bytes length binary corrupted"
        );
    }

    /**
     * Fix array stores an array whose length is upto 15 elements.
     *
     * +--------+~~~~~~~~~~~~~~~~~+
     * |1001XXXX|    N objects    |
     * +--------+~~~~~~~~~~~~~~~~~+
     */
    @Test
    void testPackFixedLengthArrays() throws IOException {
        int[] intArray = {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11};
        List<String> stringList = Arrays.asList("one", "two");
        msgPackLite.pack(intArray, outputStream);
        msgPackLite.pack(stringList, outputStream);
        msgPackLite.pack(new ArrayList<String>(), outputStream);

        byte[] expected = {
            // 12-length array
            (byte) 0x9c,
            // 12 positive fixnum elements
            (byte) 0x00, (byte) 0x01, (byte) 0x02, (byte) 0x03, (byte) 0x04, (byte) 0x05,
            (byte) 0x06, (byte) 0x07, (byte) 0x08, (byte) 0x09, (byte) 0x0a, (byte) 0x0b,
            // 2-length array
            (byte) 0x92,
            // 2 fixed string elements
            (byte) 0xa3, (byte) 0x6f, (byte) 0x6e, (byte) 0x65, // one
            (byte) 0xa3, (byte) 0x74, (byte) 0x77, (byte) 0x6f, // two,
            // empty array
            (byte) 0x90
        };
        assertArrayEquals(expected, outputStream.toByteArray());
    }

    /**
     * Array 16 stores an array whose length is upto (2^16)-1 elements.
     *
     * +--------+--------+--------+~~~~~~~~~~~~~~~~~+
     * |  0xdc  |YYYYYYYY|YYYYYYYY|    N objects    |
     * +--------+--------+--------+~~~~~~~~~~~~~~~~~+
     *
     * Array 32 stores an array whose length is upto (2^32)-1 elements.
     *
     * +--------+--------+--------+--------+--------+~~~~~~~~~~~~~~~~~+
     * |  0xdd  |ZZZZZZZZ|ZZZZZZZZ|ZZZZZZZZ|ZZZZZZZZ|    N objects    |
     * +--------+--------+--------+--------+--------+~~~~~~~~~~~~~~~~~+
     */
    @Test
    void testPackArrays() throws IOException {
        int firstBinarySize = 30003;
        byte[] twoBytesLengthArray = new byte[firstBinarySize];
        Arrays.fill(twoBytesLengthArray, (byte) 1);
        twoBytesLengthArray[0] = (byte) 0xdc;
        twoBytesLengthArray[1] = (byte) 0x75;
        twoBytesLengthArray[2] = (byte) 0x30;

        int secondBinarySize = 80005;
        byte[] fourBytesLengthArray = new byte[secondBinarySize];
        Arrays.fill(fourBytesLengthArray, (byte) 2);
        fourBytesLengthArray[0] = (byte) 0xdd;
        fourBytesLengthArray[1] = (byte) 0x00;
        fourBytesLengthArray[2] = (byte) 0x01;
        fourBytesLengthArray[3] = (byte) 0x38;
        fourBytesLengthArray[4] = (byte) 0x80;

        int[] firstArray = new int[30000];
        int[] secondArray = new int[80000];
        Arrays.fill(firstArray, 1);
        Arrays.fill(secondArray, 2);

        msgPackLite.pack(firstArray, outputStream);
        msgPackLite.pack(secondArray, outputStream);

        byte[] bytes = outputStream.toByteArray();
        assertSubArrayEquals(
            twoBytesLengthArray, 0, bytes, 0, firstBinarySize, () -> "2 bytes length binary corrupted"
        );
        assertSubArrayEquals(
            fourBytesLengthArray, 0, bytes, firstBinarySize, secondBinarySize, () -> "4 bytes length binary corrupted"
        );
    }

    /**
     * Fix map stores a map whose length is upto 15 elements.
     *
     * +--------+~~~~~~~~~~~~~~~~~+
     * |1000XXXX|   N*2 objects   |
     * +--------+~~~~~~~~~~~~~~~~~+
     */
    @Test
    void testPackFixedMaps() throws IOException {

        Map<String, Integer> map = new HashMap<>();
        map.put("one", 1);
        map.put("two", 2);

        msgPackLite.pack(map, outputStream);
        msgPackLite.pack(new HashMap<String, Integer>(), outputStream);

        byte[] expected = {
            // map with 2 entries
            (byte) 0x82,
            // first entry {one -> 1}
            (byte) 0xa3, (byte) 0x6f, (byte) 0x6e, (byte) 0x65, (byte) 0x01,
            // second entry (two -> 2)
            (byte) 0xa3, (byte) 0x74, (byte) 0x77, (byte) 0x6f, (byte) 0x02,
            // empty map
            (byte) 0x80,
        };
        assertArrayEquals(expected, outputStream.toByteArray());
    }

    /**
     * Map 16 stores a map whose length is upto (2^16)-1 elements.
     *
     * +--------+--------+--------+~~~~~~~~~~~~~~~~~+
     * |  0xde  |YYYYYYYY|YYYYYYYY|   N*2 objects   |
     * +--------+--------+--------+~~~~~~~~~~~~~~~~~+
     *
     * Map 32 stores a map whose length is upto (2^32)-1 elements.
     *
     * +--------+--------+--------+--------+--------+~~~~~~~~~~~~~~~~~+
     * |  0xdf  |ZZZZZZZZ|ZZZZZZZZ|ZZZZZZZZ|ZZZZZZZZ|   N*2 objects   |
     * +--------+--------+--------+--------+--------+~~~~~~~~~~~~~~~~~+
     */
    @Test
    void testPackMaps() throws IOException {
        int value = 1;
        int firstBinarySize = 30000 * 4 + 3;
        int secondBinarySize = 80000 * 6 + 5;
        Map<Integer, Integer> firstMap =
            new LinkedHashMap<>(firstBinarySize + (int) Math.ceil(0.25 * firstBinarySize));
        Map<Integer, Integer> secondMap =
            new LinkedHashMap<>(secondBinarySize + (int) Math.ceil(0.25 * secondBinarySize));
        byte[] twoBytesIntegerMap = new byte[firstBinarySize];

        twoBytesIntegerMap[0] = (byte) 0xde;
        twoBytesIntegerMap[1] = (byte) 0x75;
        twoBytesIntegerMap[2] = (byte) 0x30;
        int key = 10000;
        for (int i = 3; i < firstBinarySize; key++) {
            firstMap.put(key, value);
            twoBytesIntegerMap[i++] = (byte) 0xcd;
            twoBytesIntegerMap[i++] = (byte) ((key >> 8) & 0xff);
            twoBytesIntegerMap[i++] = (byte) (key & 0xff);
            twoBytesIntegerMap[i++] = (byte) value;
        }

        key = 100000;
        value = 2;
        byte[] fourBytesIntegerMap = new byte[secondBinarySize];
        fourBytesIntegerMap[0] = (byte) 0xdf;
        fourBytesIntegerMap[1] = (byte) 0x00;
        fourBytesIntegerMap[2] = (byte) 0x01;
        fourBytesIntegerMap[3] = (byte) 0x38;
        fourBytesIntegerMap[4] = (byte) 0x80;
        for (int i = 5; i < secondBinarySize; key++) {
            secondMap.put(key, value);
            fourBytesIntegerMap[i++] = (byte) 0xce;
            fourBytesIntegerMap[i++] = (byte) ((key >> 24) & 0xff);
            fourBytesIntegerMap[i++] = (byte) ((key >> 16) & 0xff);
            fourBytesIntegerMap[i++] = (byte) ((key >> 8) & 0xff);
            fourBytesIntegerMap[i++] = (byte) (key & 0xff);
            fourBytesIntegerMap[i++] = (byte) value;
        }

        msgPackLite.pack(firstMap, outputStream);
        msgPackLite.pack(secondMap, outputStream);

        byte[] bytes = outputStream.toByteArray();
        assertSubArrayEquals(
            twoBytesIntegerMap, 0, bytes, 0, firstBinarySize, () -> "2 bytes length binary corrupted"
        );
        assertSubArrayEquals(
            fourBytesIntegerMap, 0, bytes, firstBinarySize, secondBinarySize, () -> "4 bytes length binary corrupted"
        );
    }

    /**
     * {@link Callable} stores a value produced by itself
     * according to MsgPack spec.
     */
    @Test
    void testPackCallable() throws IOException {
        msgPackLite.pack((Callable<Integer>) () -> 1, outputStream);
        msgPackLite.pack((Callable<String>) () -> "hello", outputStream);
        msgPackLite.pack((Callable<Code>) () -> Code.PING, outputStream);
        msgPackLite.pack((Callable<byte[]>) () -> new byte[] { 0x01, 0x02 }, outputStream);

        byte[] expectedPositive = {
            // () -> 1
            (byte) 0x01,
            // () -> "hello"
            (byte) 0xa5, (byte) 0x68, (byte) 0x65, (byte) 0x6c, (byte) 0x6c, (byte) 0x6f,
            // () -> Code.PING (64)
            (byte) 0x40,
            // () -> { 1, 2 }
            (byte) 0xc4, (byte) 0x02, (byte) 0x01, (byte) 0x02,
        };
        assertArrayEquals(expectedPositive, outputStream.toByteArray());
    }

    @Test
    void testPackThrowingCallable() {
        Exception expectedError = new RuntimeException();
        Callable<String> throwingCallable = () -> {
            throw expectedError;
        };

        IllegalArgumentException error = assertThrows(
            IllegalArgumentException.class,
            () -> msgPackLite.pack(throwingCallable, outputStream)
        );
        assertEquals(expectedError, error.getCause());
    }

    @Test
    void testPackNumberLessThanLowerBound() {
        BigInteger smallNumber = BigInteger.valueOf(2).pow(64);
        IllegalArgumentException error = assertThrows(
            IllegalArgumentException.class,
            () -> msgPackLite.pack(smallNumber, outputStream)
        );

        assertEquals("Cannot encode BigInteger as MsgPack: out of -2^63..2^64-1 range", error.getMessage());
    }

    @Test
    void testPackNumberBiggerThanUpperBound() {
        BigInteger smallNumber = BigInteger.valueOf(Long.MIN_VALUE).subtract(BigInteger.ONE);
        IllegalArgumentException error = assertThrows(
            IllegalArgumentException.class,
            () -> msgPackLite.pack(smallNumber, outputStream)
        );

        assertEquals("Cannot encode BigInteger as MsgPack: out of -2^63..2^64-1 range", error.getMessage());
    }

    @Test
    void testPackUnsupportedClass() {
        Set<String> set = new HashSet<>();

        IllegalArgumentException error = assertThrows(
            IllegalArgumentException.class,
            () -> msgPackLite.pack(set, outputStream)
        );

        assertEquals("Cannot msgpack object of type java.util.HashSet", error.getMessage());
    }

}
