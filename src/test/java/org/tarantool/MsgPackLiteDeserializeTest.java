package org.tarantool;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * Test set for {@link MsgPackLite#unpack(InputStream)} method.
 */
public class MsgPackLiteDeserializeTest {

    private MsgPackLite msgPackLite;
    private ByteArrayInputStream inputStream;

    @BeforeEach
    void setUp() {
        msgPackLite = new MsgPackLite();
    }

    @AfterEach
    void tearDown() throws IOException {
        if (inputStream != null) {
            inputStream.close();
        }
    }

    /**
     * Unpacks {@code nil} value.
     *
     * @see MsgPackLiteSerializeTest#testPackNull()
     */
    @Test
    void testUnpackNil() throws IOException {
        inputStream = new ByteArrayInputStream(new byte[] { (byte) 0xc0 });
        assertNull(msgPackLite.unpack(inputStream));
    }

    /**
     * Unpacks booleans.
     *
     * @see MsgPackLiteSerializeTest#testPackBoolean()
     */
    @Test
    void testUnpackBoolean() throws IOException {
        inputStream = new ByteArrayInputStream(new byte[] { (byte) 0xc2, (byte) 0xc3 });

        Object value = msgPackLite.unpack(inputStream);
        assertFalse((Boolean) value);

        value = msgPackLite.unpack(inputStream);
        assertTrue((Boolean) value);
    }

    /**
     * Unpacks positive fixnum.
     *
     * @see MsgPackLiteSerializeTest#testPackFixedPositiveInteger()
     */
    @Test
    void testUnpackFixedPositiveInteger() throws IOException {
        inputStream = new ByteArrayInputStream(new byte[] { (byte) 0x0, (byte) 0x40, (byte) 0x7f });

        int[] numbers = { 0, 64, 127 };
        for (int number : numbers) {
            Object value = msgPackLite.unpack(inputStream);
            assertEquals(number, value);
        }
    }

    /**
     * Unpacks negative fixnum.
     *
     * @see MsgPackLiteSerializeTest#testPackFixedNegativeInteger()
     */
    @Test
    void testUnpackFixedNegativeInteger() throws IOException {
        inputStream = new ByteArrayInputStream(new byte[] {
            (byte) 0xe0,
            (byte) 0xf0,
            (byte) 0xff
        });
        byte[] bytes = { -32, -16, -1 };
        for (byte number : bytes) {
            Object value = msgPackLite.unpack(inputStream);
            assertEquals(number, value);
        }
    }

    /**
     * Unpacks unsigned integers.
     *
     * @see MsgPackLiteSerializeTest#testPackUnsignedIntegers()
     */
    @Test
    void testUnpackUnsignedIntegers() throws IOException {
        byte[] bytes = {
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
        inputStream = new ByteArrayInputStream(bytes);

        Object[] numbers = {
            255, 256,
            65535, 65536L,
            4294967295L, 4294967296L,
            new BigInteger("18446744073709551615")
        };

        for (Object number : numbers) {
            Object value = msgPackLite.unpack(inputStream);
            assertEquals(number, value);
        }
    }

    /**
     * Unpacks non-compact unsigned integers.
     *
     * @see MsgPackLiteSerializeTest#testPackUnsignedIntegers()
     */
    @Test
    void testUnpackNonCompactUnsignedIntegers() throws IOException {
        byte[] bytes = {
            // uint8 - 1
            (byte) 0xcc, (byte) 0x01,
            // uint16 - 1
            (byte) 0xcd, (byte) 0x00, (byte) 0x01,
            // uint32 - 1
            (byte) 0xce, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x01,
            // uint64 - 1
            (byte) 0xcf, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00,
            (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x01,
        };
        inputStream = new ByteArrayInputStream(bytes);

        Object[] numbers = {
            1, 1, 1L, 1L
        };

        for (Object number : numbers) {
            Object value = msgPackLite.unpack(inputStream);
            assertEquals(number, value);
        }
    }

    /**
     * Unpacks signed integers.
     *
     * @see MsgPackLiteSerializeTest#testPackSignedIntegers()
     */
    @Test
    void testUnpackSignedIntegers() throws IOException {
        inputStream = new ByteArrayInputStream(new byte[] {
            // 127 (2^7-1)
            (byte) 0xd0, (byte) 0x7f,
            // 128 (2^7)
            (byte) 0xd1, (byte) 0x00, (byte) 0x80,
            // -128 (-2^7)
            (byte) 0xd0, (byte) 0x80,
            // -129 (-2^7-1)
            (byte) 0xd1, (byte) 0xff, (byte) 0x7f,
            // 32767 (2^15-1)
            (byte) 0xd1, (byte) 0x7f, (byte) 0xff,
            // 32768 (2^15)
            (byte) 0xd2, (byte) 0x00, (byte) 0x00, (byte) 0x80, (byte) 0x00,
            // -32768 (-2^15)
            (byte) 0xd1, (byte) 0x80, (byte) 0x00,
            // -32769 (-2^15-1)
            (byte) 0xd2, (byte) 0xff, (byte) 0xff, (byte) 0x7f, (byte) 0xff,
            // 2147483647 (2^31-1)
            (byte) 0xd2, (byte) 0x7f, (byte) 0xff, (byte) 0xff, (byte) 0xff,
            // 2147483648 (2^31)
            (byte) 0xd3, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00,
            (byte) 0x80, (byte) 0x00, (byte) 0x00, (byte) 0x00,
            // -2147483649 (-2^31)
            (byte) 0xd2, (byte) 0x80, (byte) 0x00, (byte) 0x00, (byte) 0x00,
            // -2147483649 (-2^31-1)
            (byte) 0xd3, (byte) 0xff, (byte) 0xff, (byte) 0xff, (byte) 0xff,
            (byte) 0x7f, (byte) 0xff, (byte) 0xff, (byte) 0xff,
            // 9223372036854775807 (2^63-1)
            (byte) 0xd3, (byte) 0x7f, (byte) 0xff, (byte) 0xff, (byte) 0xff,
            (byte) 0xff, (byte) 0xff, (byte) 0xff, (byte) 0xff,
            // -9223372036854775808 (-2^63)
            (byte) 0xd3, (byte) 0x80, (byte) 0x00, (byte) 0x00, (byte) 0x00,
            (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00,
        });

        Object[] numbers = {
            (byte) 127, (short) 128, (byte) -128, (short) -129,
            (short) 32767, 32768, (short) -32768, -32769,
            2147483647, 2147483648L, -2147483648, -2147483649L,
            9223372036854775807L, -9223372036854775808L
        };

        for (Object number : numbers) {
            Object value = msgPackLite.unpack(inputStream);
            assertEquals(number, value);
        }
    }

    /**
     * Unpacks non-compact signed integers.
     *
     * @see MsgPackLiteSerializeTest#testPackSignedIntegers() ()
     */
    @Test
    void testUnpackNonCompactSignedIntegers() throws IOException {
        byte[] bytes = {
            // int8 - 1
            (byte) 0xd0, (byte) 0x01,
            // int16 - 1
            (byte) 0xd1, (byte) 0x00, (byte) 0x01,
            // int32 - 1
            (byte) 0xd2, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x01,
            // int64 - 1
            (byte) 0xd3, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00,
            (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x01,
        };
        inputStream = new ByteArrayInputStream(bytes);

        Object[] numbers = {
            (byte) 1, (short) 1, 1, 1L
        };

        for (Object number : numbers) {
            Object value = msgPackLite.unpack(inputStream);
            assertEquals(number, value);
        }
    }

    /**
     * Unpacks floats.
     *
     * @see MsgPackLiteSerializeTest#testPackFloatNumber()
     */
    @Test
    void testUnpackFloatNumber() throws IOException {
        inputStream = new ByteArrayInputStream(new byte[] {
            // NaN
            (byte) 0xca, (byte) 0x7f, (byte) 0xc0, (byte) 0x00, (byte) 0x00,
            // -Inf
            (byte) 0xca, (byte) 0xff, (byte) 0x80, (byte) 0x00, (byte) 0x00,
            // Inf
            (byte) 0xca, (byte) 0x7f, (byte) 0x80, (byte) 0x00, (byte) 0x00,
            // 0.0
            (byte) 0xca, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00,
            // -0.0
            (byte) 0xca, (byte) 0x80, (byte) 0x00, (byte) 0x00, (byte) 0x00,
            // 60.5
            (byte) 0xca, (byte) 0x42, (byte) 0x72, (byte) 0x00, (byte) 0x00,
            // -100
            (byte) 0xca, (byte) 0xc2, (byte) 0xc8, (byte) 0x00, (byte) 0x00,
        });

        assertEquals(Float.NaN, msgPackLite.unpack(inputStream));
        assertEquals(Float.NEGATIVE_INFINITY, msgPackLite.unpack(inputStream));
        assertEquals(Float.POSITIVE_INFINITY, msgPackLite.unpack(inputStream));
        assertEquals(0.0f, msgPackLite.unpack(inputStream));
        assertEquals(-0.0f, msgPackLite.unpack(inputStream));
        assertEquals(60.5f, msgPackLite.unpack(inputStream));
        assertEquals(-100.0f, msgPackLite.unpack(inputStream));
    }

    /**
     * Unpacks doubles.
     *
     * @see MsgPackLiteSerializeTest#testPackDoubleNumber()
     */
    @Test
    void testUnpackDoubleNumber() throws IOException {
        inputStream = new ByteArrayInputStream(new byte[] {
            // NaN
            (byte) 0xcb, (byte) 0x7f, (byte) 0xf8, (byte) 0x00, (byte) 0x00,
            (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00,
            // -Inf
            (byte) 0xcb, (byte) 0xff, (byte) 0xf0, (byte) 0x00, (byte) 0x00,
            (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00,
            // Inf
            (byte) 0xcb, (byte) 0x7f, (byte) 0xf0, (byte) 0x00, (byte) 0x00,
            (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00,
            // 0.0
            (byte) 0xcb, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00,
            (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00,
            // -0.0
            (byte) 0xcb, (byte) 0x80, (byte) 0x00, (byte) 0x00, (byte) 0x00,
            (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00,
            // -213.125
            (byte) 0xcb, (byte) 0xc0, (byte) 0x6a, (byte) 0xa4, (byte) 0x00,
            (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00,
            // 100.5
            (byte) 0xcb, (byte) 0x40, (byte) 0x59, (byte) 0x20, (byte) 0x00,
            (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00,
        });

        assertEquals(Double.NaN, msgPackLite.unpack(inputStream));
        assertEquals(Double.NEGATIVE_INFINITY, msgPackLite.unpack(inputStream));
        assertEquals(Double.POSITIVE_INFINITY, msgPackLite.unpack(inputStream));
        assertEquals(0.0d, msgPackLite.unpack(inputStream));
        assertEquals(-0.0d, msgPackLite.unpack(inputStream));
        assertEquals(-213.125d, msgPackLite.unpack(inputStream));
        assertEquals(100.5d, msgPackLite.unpack(inputStream));
    }

    /**
     * Unpacks fixed strings.
     *
     * @see MsgPackLiteSerializeTest#testPackFixedStrings() ()
     */
    @Test
    void testUnpackFixedStrings() throws IOException {
        inputStream = new ByteArrayInputStream(new byte[] {
            // latin "a"
            (byte) 0xa1, (byte) 0x61,
            // latin "hello"
            (byte) 0xa5, (byte) 0x68, (byte) 0x65, (byte) 0x6c, (byte) 0x6c, (byte) 0x6f,
            // cyrillic "привет" (12 bytes in utf-8)
            (byte) 0xac, (byte) 0xd0, (byte) 0xbf, (byte) 0xd1, (byte) 0x80, (byte) 0xd0,
            (byte) 0xb8, (byte) 0xd0, (byte) 0xb2, (byte) 0xd0, (byte) 0xb5, (byte) 0xd1, (byte) 0x82,
        });

        assertEquals("a", msgPackLite.unpack(inputStream));
        assertEquals("hello", msgPackLite.unpack(inputStream));
        assertEquals("привет", msgPackLite.unpack(inputStream));
    }

    /**
     * Unpacks strings.
     *
     * @see MsgPackLiteSerializeTest#testPackStrings()
     */
    @Test
    void testUnpackStrings() throws IOException {
        byte[] oneByteLength = new byte[202];
        Arrays.fill(oneByteLength, (byte) 'a');
        oneByteLength[0] = (byte) 0xd9;
        oneByteLength[1] = (byte) 0xc8;
        inputStream = new ByteArrayInputStream(oneByteLength);
        assertEquals(new String(oneByteLength, 2, 200, StandardCharsets.UTF_8), msgPackLite.unpack(inputStream));

        byte[] twoBytesLength = new byte[30003];
        Arrays.fill(twoBytesLength, (byte) 'b');
        twoBytesLength[0] = (byte) 0xda;
        twoBytesLength[1] = (byte) 0x75;
        twoBytesLength[2] = (byte) 0x30;
        inputStream = new ByteArrayInputStream(twoBytesLength);
        assertEquals(new String(twoBytesLength, 3, 30000, StandardCharsets.UTF_8), msgPackLite.unpack(inputStream));

        byte[] fourBytesLength = new byte[80005];
        Arrays.fill(fourBytesLength, (byte) 'c');
        fourBytesLength[0] = (byte) 0xdb;
        fourBytesLength[1] = (byte) 0x00;
        fourBytesLength[2] = (byte) 0x01;
        fourBytesLength[3] = (byte) 0x38;
        fourBytesLength[4] = (byte) 0x80;
        inputStream = new ByteArrayInputStream(fourBytesLength);
        assertEquals(new String(fourBytesLength, 5, 80000, StandardCharsets.UTF_8), msgPackLite.unpack(inputStream));
    }

    /**
     * Unpacks non-compact strings.
     *
     * @see MsgPackLiteSerializeTest#testPackStrings()
     */
    @Test
    void testUnpackNonCompactStrings() throws IOException {
        inputStream = new ByteArrayInputStream(new byte[] {
            // string8 "a"
            (byte) 0xd9, (byte) 0x01, (byte) 0x61,
            // string16 "a"
            (byte) 0xda, (byte) 0x00, (byte) 0x01, (byte) 0x61,
            // string32 "a"
            (byte) 0xdb, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x01, (byte) 0x61
        });

        for (int i = 0; i < 3; i++) {
            Object string = msgPackLite.unpack(inputStream);
            assertEquals("a", string);
        }
    }

    /**
     * Unpacks binaries.
     * Here is no intention to parse those bytes in some way, just be sure
     * that binaries are got correctly (they have right size and content) in
     * scope of MsgPack support. Nonetheless, in the real world binaries can be
     * treated as PDF-documents, images, and other sort of binary data.
     *
     * @see MsgPackLiteSerializeTest#testPackBinArrays()
     */
    @Test
    void testUnpackBinArrays() throws IOException {
        byte[] oneByteLength = new byte[202];
        oneByteLength[0] = (byte) 0xc4;
        oneByteLength[1] = (byte) 0xc8;
        byte b = 0;
        for (int i = 2; i < 202; i++) {
            oneByteLength[i] = b++;
        }
        byte[] firstBinary = new byte[200];
        System.arraycopy(oneByteLength, 2, firstBinary, 0, 200);
        inputStream = new ByteArrayInputStream(oneByteLength);
        assertArrayEquals(firstBinary, (byte[]) msgPackLite.unpack(inputStream));

        byte[] twoBytesLength = new byte[30003];
        twoBytesLength[0] = (byte) 0xc5;
        twoBytesLength[1] = (byte) 0x75;
        twoBytesLength[2] = (byte) 0x30;
        b = 0;
        for (int i = 3; i < 30003; i++) {
            twoBytesLength[i] = b++;
        }
        byte[] secondBinary = new byte[30000];
        System.arraycopy(twoBytesLength, 3, secondBinary, 0, 30000);
        inputStream = new ByteArrayInputStream(twoBytesLength);
        assertArrayEquals(secondBinary, (byte[]) msgPackLite.unpack(inputStream));

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
        byte[] thirdBinary = new byte[80000];
        System.arraycopy(fourBytesLength, 5, thirdBinary, 0, 80000);
        inputStream = new ByteArrayInputStream(fourBytesLength);
        assertArrayEquals(thirdBinary, (byte[]) msgPackLite.unpack(inputStream));
    }

    /**
     * Unpacks non-compact binary data.
     *
     * @see MsgPackLiteSerializeTest#testPackBinArrays()
     * @see #testUnpackBinArrays()
     */
    @Test
    void testUnpackNonCompactBinaries() throws IOException {
        inputStream = new ByteArrayInputStream(new byte[] {
            // binary8 [ 0x01 ]
            (byte) 0xc4, (byte) 0x01, (byte) 0x01,
            // binary16 [ 0x01 ]
            (byte) 0xc5, (byte) 0x00, (byte) 0x01, (byte) 0x01,
            // binary32 [ 0x01 ]
            (byte) 0xc6, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x01, (byte) 0x01
        });

        byte[] bytes = { 0x01 };
        for (int i = 0; i < 3; i++) {
            byte[] value = (byte[]) msgPackLite.unpack(inputStream);
            assertArrayEquals(bytes, value);
        }
    }

    /**
     * Unpacks fixed size arrays.
     *
     * @see MsgPackLiteSerializeTest#testPackFixedLengthArrays()
     */
    @Test
    void testUnpackFixedLengthArrays() throws IOException {
        inputStream = new ByteArrayInputStream(new byte[] {
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
        });

        List<Integer> integerList = Arrays.asList(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11);
        List<String> stringList = Arrays.asList("one", "two");
        assertEquals(integerList, msgPackLite.unpack(inputStream));
        assertEquals(stringList, msgPackLite.unpack(inputStream));
    }

    /**
     * Unpacks arrays.
     *
     * @see MsgPackLiteSerializeTest#testPackArrays()
     */
    @Test
    void testUnpackArrays() throws IOException {
        int firstBinarySize = 30003;
        byte[] twoBytesLengthArray = new byte[firstBinarySize];
        Arrays.fill(twoBytesLengthArray, (byte) 1);
        twoBytesLengthArray[0] = (byte) 0xdc;
        twoBytesLengthArray[1] = (byte) 0x75;
        twoBytesLengthArray[2] = (byte) 0x30;
        List<Integer> firstList = IntStream.generate(() -> 1)
            .limit(30000)
            .boxed()
            .collect(Collectors.toList());
        inputStream = new ByteArrayInputStream(twoBytesLengthArray);
        assertEquals(firstList, msgPackLite.unpack(inputStream));

        int secondBinarySize = 80005;
        byte[] fourBytesLengthArray = new byte[secondBinarySize];
        Arrays.fill(fourBytesLengthArray, (byte) 2);
        fourBytesLengthArray[0] = (byte) 0xdd;
        fourBytesLengthArray[1] = (byte) 0x00;
        fourBytesLengthArray[2] = (byte) 0x01;
        fourBytesLengthArray[3] = (byte) 0x38;
        fourBytesLengthArray[4] = (byte) 0x80;
        List<Integer> secondList = IntStream.generate(() -> 2)
            .limit(80000)
            .boxed()
            .collect(Collectors.toList());
        inputStream = new ByteArrayInputStream(fourBytesLengthArray);
        assertEquals(secondList, msgPackLite.unpack(inputStream));
    }

    /**
     * Unpacks non-compact arrays.
     *
     * @see MsgPackLiteSerializeTest#testPackArrays()
     */
    @Test
    void testUnpackNonCompactArrays() throws IOException {
        inputStream = new ByteArrayInputStream(new byte[] {
            // array16 [ 1 ]
            (byte) 0xdc, (byte) 0x00, (byte) 0x01, (byte) 0x01,
            // array32 [ 1 ]
            (byte) 0xdd, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x01, (byte) 0x01
        });

        List<Integer> array = new ArrayList<>();
        array.add(1);
        for (int i = 0; i < 2; i++) {
            Object value = msgPackLite.unpack(inputStream);
            assertEquals(array, value);
        }
    }

    /**
     * Unpacks fixed maps.
     *
     * @see MsgPackLiteSerializeTest#testPackFixedMaps()
     */
    @Test
    void testUnpackFixedMaps() throws IOException {
        inputStream = new ByteArrayInputStream(new byte[] {
            // map with 2 entries
            (byte) 0x82,
            // first entry {one: 1}
            (byte) 0xa3, (byte) 0x6f, (byte) 0x6e, (byte) 0x65, (byte) 0x01,
            // second entry (two: 2)
            (byte) 0xa3, (byte) 0x74, (byte) 0x77, (byte) 0x6f, (byte) 0x02,
            // empty map
            (byte) 0x80,
        });

        Map<String, Integer> map = new HashMap<>();
        map.put("one", 1);
        map.put("two", 2);

        assertEquals(map, msgPackLite.unpack(inputStream));
        assertEquals(new HashMap<String, Integer>(), msgPackLite.unpack(inputStream));
    }

    /**
     * Unpacks maps.
     *
     * @see MsgPackLiteSerializeTest#testPackMaps()
     */
    @Test
    void testUnpackMaps() throws IOException {
        int value = 1;
        int firstBinarySize = 30000 * 4 + 3;

        Map<Integer, Integer> firstMap =
            new HashMap<>(firstBinarySize + (int) Math.ceil(0.25 * firstBinarySize));
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
        inputStream = new ByteArrayInputStream(twoBytesIntegerMap);
        assertEquals(firstMap, msgPackLite.unpack(inputStream));

        key = 100000;
        value = 2;
        int secondBinarySize = 80000 * 6 + 5;
        Map<Long, Integer> secondMap =
            new HashMap<>(secondBinarySize + (int) Math.ceil(0.25 * secondBinarySize));
        byte[] fourBytesIntegerMap = new byte[secondBinarySize];
        fourBytesIntegerMap[0] = (byte) 0xdf;
        fourBytesIntegerMap[1] = (byte) 0x00;
        fourBytesIntegerMap[2] = (byte) 0x01;
        fourBytesIntegerMap[3] = (byte) 0x38;
        fourBytesIntegerMap[4] = (byte) 0x80;
        for (int i = 5; i < secondBinarySize; key++) {
            secondMap.put((long) key, value);
            fourBytesIntegerMap[i++] = (byte) 0xce;
            fourBytesIntegerMap[i++] = (byte) ((key >> 24) & 0xff);
            fourBytesIntegerMap[i++] = (byte) ((key >> 16) & 0xff);
            fourBytesIntegerMap[i++] = (byte) ((key >> 8) & 0xff);
            fourBytesIntegerMap[i++] = (byte) (key & 0xff);
            fourBytesIntegerMap[i++] = (byte) value;
        }
        inputStream = new ByteArrayInputStream(fourBytesIntegerMap);
        assertEquals(secondMap, msgPackLite.unpack(inputStream));
    }

    /**
     * Unpacks non-compact maps.
     *
     * @see MsgPackLiteSerializeTest#testPackMaps()
     */
    @Test
    void testUnpackNonCompactMaps() throws IOException {
        inputStream = new ByteArrayInputStream(new byte[] {
            // map16 {one: 1}
            (byte) 0xde, (byte) 0x00, (byte) 0x01, (byte) 0xa3, (byte) 0x6f, (byte) 0x6e, (byte) 0x65, (byte) 0x01,
            // map32 {one: 1}
            (byte) 0xdf, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x01,
            (byte) 0xa3, (byte) 0x6f, (byte) 0x6e, (byte) 0x65, (byte) 0x01,
        });

        Map<String, Integer> map = new HashMap<>();
        map.put("one", 1);
        for (int i = 0; i < 2; i++) {
            Object value = msgPackLite.unpack(inputStream);
            assertEquals(map, value);
        }
    }

    /**
     * Unpacks too short packed homogeneous data (integers, strings, binaries).
     */
    @Test
    void testUnpackTooShortVariableLengthObjects() {
        // expected a 64-bit uint but serialized as a cut two bytes sequence
        byte[] shortUint64 = { (byte) 0xcf, (byte) 0x00, (byte) 0x00 };
        inputStream = new ByteArrayInputStream(shortUint64);
        assertThrows(EOFException.class, () -> msgPackLite.unpack(inputStream));

        // expected a 4 character string but serialized as "abc"
        byte[] shortString = { (byte) 0xd9, (byte) 0x04, (byte) 0x61, (byte) 0x62, (byte) 0x63 };
        inputStream = new ByteArrayInputStream(shortString);
        assertThrows(EOFException.class, () -> msgPackLite.unpack(inputStream));

        // expected a two bytes array but serialized as a single byte array [ 0x01 ]
        byte[] shortBinary = { (byte) 0xc4, (byte) 0x02, (byte) 0x01 };
        inputStream = new ByteArrayInputStream(shortBinary);
        assertThrows(EOFException.class, () -> msgPackLite.unpack(inputStream));
    }

    /**
     * Unpacks too short complex data (arrays, maps).
     */
    @Test
    void testUnpackTooShortComplexObjects() {
        // expected a three int array but serialized as a two int array [ 1, 2 ]
        byte[] shortArray = { (byte) 0xdc, (byte) 0x00, (byte) 0x03, (byte) 0x01, (byte) 0x02 };
        inputStream = new ByteArrayInputStream(shortArray);
        IllegalArgumentException error = assertThrows(
            IllegalArgumentException.class,
            () -> msgPackLite.unpack(inputStream)
        );
        assertEquals("No more input available when expecting a value", error.getMessage());

        // expected two pairs map but serialized as a single pair map { 1: "1" }
        byte[] shortMap = { (byte) 0xde, (byte) 0x00, (byte) 0x02, (byte) 0x01, (byte) 0xa1, (byte) 0x31 };
        inputStream = new ByteArrayInputStream(shortMap);
        error = assertThrows(IllegalArgumentException.class, () -> msgPackLite.unpack(inputStream));
        assertEquals("No more input available when expecting a value", error.getMessage());
    }

    /**
     * Unpacks too large packed homogeneous data (strings, binaries).
     */
    @Test
    void testUnpackTooLargeVariableLengthObjects() {
        // expected a 4278190080 (> 2^31-1) string but cannot be handled properly in Java
        // let's serialize just first character because size >= 2^31 is enough to abort parsing fail-fast
        byte[] largeString = { (byte) 0xdb, (byte) 0xff, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x61 };
        inputStream = new ByteArrayInputStream(largeString);
        IllegalArgumentException error = assertThrows(
            IllegalArgumentException.class,
            () -> msgPackLite.unpack(inputStream)
        );
        assertEquals("byte[] to unpack too large for Java (more than 2^31-1 elements)!", error.getMessage());

        // expected a 4278190080 (> 2^31-1) binary array but cannot be handled properly in Java
        // let's serialize just first byte because size >= 2^31 is enough to abort parsing fail-fast
        byte[] largeBinary = { (byte) 0xc6, (byte) 0xff, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x01 };
        inputStream = new ByteArrayInputStream(largeBinary);
        error = assertThrows(IllegalArgumentException.class, () -> msgPackLite.unpack(inputStream));
        assertEquals("byte[] to unpack too large for Java (more than 2^31-1 elements)!", error.getMessage());
    }

    /**
     * Unpacks too large complex data (arrays, maps).
     */
    @Test
    void testUnpackTooLargeComplexObjects() {
        // expected a 4278190080 (> 2^31-1) int array but cannot be handled properly in Java
        // let's serialize just first element because size >= 2^31 is enough to abort parsing fail-fast
        byte[] shortArray = { (byte) 0xdd, (byte) 0xff, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x01 };
        inputStream = new ByteArrayInputStream(shortArray);
        IllegalArgumentException error = assertThrows(
            IllegalArgumentException.class,
            () -> msgPackLite.unpack(inputStream)
        );
        assertEquals("Array to unpack too large for Java (more than 2^31-1 elements)!", error.getMessage());

        // expected a 4278190080 (> 2^31-1) elements map but cannot be handled properly in Java
        // let's serialize just first pair because size >= 2^31 is enough to abort parsing fail-fast
        byte[] shortMap = {
            (byte) 0xdf,
            (byte) 0xff, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x01, (byte) 0xa1, (byte) 0x31
        };
        inputStream = new ByteArrayInputStream(shortMap);
        error = assertThrows(IllegalArgumentException.class, () -> msgPackLite.unpack(inputStream));
        assertEquals("Map to unpack too large for Java (more than 2^31-1 elements)!", error.getMessage());
    }

    /**
     * Unpacks invalid object.
     */
    @Test
    void testUnpackInvalidObject() {
        // MsgPack says 0xc1 is never used to represent any objects
        byte[] invalidBytes = { (byte) 0xc1, (byte) 0xff, (byte) 0x00 };
        inputStream = new ByteArrayInputStream(invalidBytes);
        IllegalArgumentException error = assertThrows(
            IllegalArgumentException.class,
            () -> msgPackLite.unpack(inputStream)
        );
        assertTrue(error.getMessage().startsWith("Input contains invalid type"));
    }
}
