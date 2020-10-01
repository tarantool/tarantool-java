package org.tarantool.conversion;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.tarantool.conversion.DefaultConverterRegistryTest.ConversionTypesTester.testTypesForRegistry;
import static org.tarantool.conversion.DefaultConverterRegistryTest.ConversionValuesTester.testValuesForRegistry;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.math.BigInteger;

/**
 * Tests supported converters for {@link DefaultConverterRegistry}.
 */
class DefaultConverterRegistryTest {

    private ConverterRegistry converterRegistry;

    @BeforeEach
    void setUp() {
        converterRegistry = new DefaultConverterRegistry();
    }

    @Test
    void testIsConvertibleToByte() {
        testTypesForRegistry(converterRegistry)
            .from(
                Short.class, Integer.class, Long.class, Float.class,
                Double.class, BigInteger.class, BigDecimal.class, String.class
            )
            .notFrom(Boolean.class, byte[].class)
            .to(Byte.class)
            .assertTypes();
    }

    @Test
    void testToByteConversions() {
        TestablePair[] pairs = {
            new TestablePair((short) 127, (byte) 127), new TestablePair((short) -128, (byte) -128),
            new TestablePair(127, (byte) 127), new TestablePair(-128, (byte) -128),
            new TestablePair(127L, (byte) 127), new TestablePair(-128L, (byte) -128),

            new TestablePair(127.0f, (byte) 127), new TestablePair(-128.0f, (byte) -128),
            new TestablePair(127.0d, (byte) 127), new TestablePair(-128.0d, (byte) -128),

            new TestablePair(BigInteger.valueOf(127), (byte) 127),
            new TestablePair(BigInteger.valueOf(-128), (byte) -128),

            new TestablePair(BigDecimal.valueOf(127.0), (byte) 127),
            new TestablePair(BigDecimal.valueOf(-128.0), (byte) -128),

            new TestablePair("127", (byte) 127),
            new TestablePair("-128", (byte) -128),
        };

        testValuesForRegistry(converterRegistry)
            .fromValues(pairs)
            .to(Byte.class)
            .assertValues();
    }

    @Test
    void testTooLargeNumbersToByteConversions() {
        Object[] unsupportedValues = {
            (short) 128, (short) -129,
            128, -129,
            128L, -129L,
            128.0f, -129.0f, 127.5f, -128.5f, 30.125f,
            128.0d, -129.0d, 127.5d, -128.5d, 60.25d,
            BigInteger.valueOf(128), BigInteger.valueOf(-129),
            BigDecimal.valueOf(128.0), BigDecimal.valueOf(-129.0),
            BigDecimal.valueOf(127.5), BigDecimal.valueOf(-128.5), BigDecimal.valueOf(12.4),
            "128", "-129", "127.0", "127.5",
        };

        testValuesForRegistry(converterRegistry)
            .fromNotConvertibleValues(unsupportedValues)
            .to(Byte.class)
            .assertNotConvertible();
    }

    @Test
    void testIsConvertibleToShort() {
        testTypesForRegistry(converterRegistry)
            .from(
                Byte.class, Integer.class, Long.class, Float.class,
                Double.class, BigInteger.class, BigDecimal.class, String.class
            )
            .notFrom(Boolean.class, byte[].class)
            .to(Short.class)
            .assertTypes();
    }

    @Test
    void testToShortConversions() {
        TestablePair[] pairs = {
            new TestablePair((byte) 127, (short) 127), new TestablePair((byte) -128, (short) -128),
            new TestablePair(32767, (short) 32767), new TestablePair(-32768, (short) -32768),
            new TestablePair(32767L, (short) 32767), new TestablePair(-32768L, (short) -32768),

            new TestablePair(32767.0f, (short) 32767), new TestablePair(-32768.0f, (short) -32768),
            new TestablePair(32767.0d, (short) 32767), new TestablePair(-32768.0d, (short) -32768),

            new TestablePair(BigInteger.valueOf(32767), (short) 32767),
            new TestablePair(BigInteger.valueOf(-32768), (short) -32768),

            new TestablePair(BigDecimal.valueOf(32767.0), (short) 32767),
            new TestablePair(BigDecimal.valueOf(-32768.0), (short) -32768),

            new TestablePair("32767", (short) 32767),
            new TestablePair("-32768", (short) -32768),
        };

        testValuesForRegistry(converterRegistry)
            .fromValues(pairs)
            .to(Short.class)
            .assertValues();
    }

    @Test
    void testTooLargeNumbersToShortConversions() {
        Object[] unsupportedValues = {
            32768, -32769,
            32768L, -32769L,
            32768.0f, -32769.0f, 32767.4f, -32768.5f, 650.8f,
            32768.0d, -32769.0d, 32767.3d, -32768.5d, 125.7d,
            BigInteger.valueOf(32768), BigInteger.valueOf(-32769),
            BigDecimal.valueOf(32768.0), BigDecimal.valueOf(-32769.0),
            BigDecimal.valueOf(32767.4), BigDecimal.valueOf(-32768.5), BigDecimal.valueOf(100.7),
            "32768", "-32769", "32768.0", "32768.5",
        };

        testValuesForRegistry(converterRegistry)
            .fromNotConvertibleValues(unsupportedValues)
            .to(Byte.class)
            .assertNotConvertible();
    }

    @Test
    void testIsConvertibleToInt() {
        testTypesForRegistry(converterRegistry)
            .from(
                Byte.class, Short.class, Long.class, Float.class,
                Double.class, BigInteger.class, BigDecimal.class, String.class
            )
            .notFrom(Boolean.class, byte[].class)
            .to(Integer.class)
            .assertTypes();
    }

    @Test
    void testToIntConversions() {
        TestablePair[] pairs = {
            new TestablePair((byte) 127, 127), new TestablePair((byte) -128, -128),
            new TestablePair((short) 32767, 32767), new TestablePair((short) -32768, -32768),
            new TestablePair(2147483647L, 2147483647), new TestablePair(-2147483648L, -2147483648),

            // (equal to Float.intBitsToFloat(0x4EFFFFFF) - max integral float within 2^30..2^31-1 range)
            new TestablePair(2147483520.0f, 2147483520),
            new TestablePair(-2147483648.0f, -2147483648),

            new TestablePair(2147483647.0d, 2147483647),
            new TestablePair(-2147483648.0d, -2147483648),

            new TestablePair(BigInteger.valueOf(2147483647), 2147483647),
            new TestablePair(BigInteger.valueOf(-2147483648), -2147483648),

            new TestablePair(BigDecimal.valueOf(2147483647.0), 2147483647),
            new TestablePair(BigDecimal.valueOf(-2147483648.0), -2147483648),

            new TestablePair("2147483647", 2147483647),
            new TestablePair("-2147483648", -2147483648),
        };

        testValuesForRegistry(converterRegistry)
            .fromValues(pairs)
            .to(Integer.class)
            .assertValues();
    }

    @Test
    void testTooLargeNumbersToIntConversions() {
        Object[] unsupportedValues = {
            2147483648L, -2147483649L,
            120.6f, 2147483648.0f,
            150.6d, 2147483648.0d, -2147483649.0d, 2147483647.5d, -2147483648.5d,
            BigInteger.valueOf(2147483648L), BigInteger.valueOf(-2147483649L),
            BigDecimal.valueOf(560.5), BigDecimal.valueOf(2147483648.0), BigDecimal.valueOf(-2147483649.0),
            BigDecimal.valueOf(2147483647.5), BigDecimal.valueOf(-2147483648.5d),
            "2147483648", "-2147483649", "2147483647.0", "-2147483648.0", "102.6"
        };

        testValuesForRegistry(converterRegistry)
            .fromNotConvertibleValues(unsupportedValues)
            .to(Integer.class)
            .assertNotConvertible();
    }

    @Test
    void testIsConvertibleToLong() {
        testTypesForRegistry(converterRegistry)
            .from(
                Byte.class, Short.class, Integer.class, Float.class,
                Double.class, BigInteger.class, BigDecimal.class, String.class
            )
            .notFrom(Boolean.class, byte[].class)
            .to(Long.class)
            .assertTypes();
    }

    @Test
    void testToLongConversions() {
        TestablePair[] pairs = {
            new TestablePair((byte) 127, 127L), new TestablePair((byte) -128, -128L),
            new TestablePair((short) 32767, 32767L), new TestablePair((short) -32768, -32768L),
            new TestablePair(2147483647, 2147483647L), new TestablePair(-2147483648, -2147483648L),

            // (equal to Float.intBitsToFloat(0x5EFFFFFF) - max integral float within 2^62..2^63-1 range)
            new TestablePair(9223371487098961920.0f, 9223371487098961920L),
            new TestablePair(-9223372036854775808.0f, -9223372036854775808L),

            // (equal to Double.longBitsToDouble(0x43DFFFFFFFFFFFFFL) - max integral double within 2^62..2^63-1 range)
            new TestablePair(9223372036854774784.0d, 9223372036854774784L),
            new TestablePair(-9223372036854775808.0d, -9223372036854775808L),

            new TestablePair(BigInteger.valueOf(9223372036854775807L), 9223372036854775807L),
            new TestablePair(BigInteger.valueOf(-9223372036854775808L), -9223372036854775808L),

            new TestablePair(new BigDecimal("9223372036854775807.0"), 9223372036854775807L),
            new TestablePair(new BigDecimal("-9223372036854775808.0"), -9223372036854775808L),

            new TestablePair("9223372036854775807", 9223372036854775807L),
            new TestablePair("-9223372036854775808", -9223372036854775808L),
        };

        testValuesForRegistry(converterRegistry)
            .fromValues(pairs)
            .to(Long.class)
            .assertValues();
    }

    @Test
    void testTooLargeNumbersToLongConversions() {
        Object[] unsupportedValues = {
            9223372036854775808.0f,
            12.6f, 18_446_744_073_709_551_616.0f, -18_446_744_073_709_551_616.0f,
            12.6d, 18_446_744_073_709_551_616.0d, -18_446_744_073_709_551_616.0d,
            new BigInteger("9223372036854775808"), new BigInteger("-9223372036854775809"),
            new BigDecimal("9223372036854775808.0"), new BigDecimal("-9223372036854775809.0"),
            new BigDecimal("12.6"), new BigDecimal("-34.2"),
            "9223372036854775808", "-9223372036854775809", "12.6"
        };

        testValuesForRegistry(converterRegistry)
            .fromNotConvertibleValues(unsupportedValues)
            .to(Long.class)
            .assertNotConvertible();
    }

    @Test
    void testIsConvertibleToFloat() {
        testTypesForRegistry(converterRegistry)
            .from(
                Byte.class, Short.class, Integer.class, Long.class,
                Double.class, BigInteger.class, BigDecimal.class, String.class
            )
            .notFrom(Boolean.class, byte[].class)
            .to(Float.class)
            .assertTypes();
    }

    @Test
    void testToFloatConversions() {
        TestablePair[] pairs = {
            new TestablePair((byte) 127, 127.0f), new TestablePair((byte) -128, -128.0f),
            new TestablePair((short) 32767, 32767.0f), new TestablePair((short) -32768, -32768.0f),
            // max int cannot be accommodated exactly,
            // the last int less than max int being exactly representable is 2147483520
            // (equal to Float.intBitsToFloat(0x4EFFFFFF) - max integral float within 2^30..2^31-1 range)
            new TestablePair(2147483520, 2147483520.0f),
            // the pen-ultimate int less than max int being exactly representable is 2147483392
            // (equal to Float.intBitsToFloat(0x4EFFFFFE)
            new TestablePair(2147483392, 2147483392.0f),
            new TestablePair(-2147483648, -2147483648.0f),

            new TestablePair(2147483520L, 2147483520.0f),
            // (equal to Float.intBitsToFloat(0x5EFFFFFF) - max integral float within 2^62..2^63-1 range)
            new TestablePair(9223371487098961920L, 9223371487098961920.0f),
            new TestablePair(-9223372036854775808L, -9223372036854775808.0f),

            new TestablePair(2147483648.0d, 2147483648.0f),
            new TestablePair(-2147483648.0d, -2147483648.0f),

            new TestablePair(new BigInteger("18446744073709551616"), 18446744073709551616.0f),
            // (equal to Float.intBitsToFloat(0x5F7FFFFF) - max integral float within 2^63..2^64-1 range)
            new TestablePair(new BigInteger("18446742974197923840"), 18446742974197923840.0f),
            new TestablePair(new BigInteger("-18446744073709551616"), -18446744073709551616.0f),

            new TestablePair(new BigDecimal("18446744073709551616.0"), 18446744073709551616.0f),
            new TestablePair(new BigDecimal("-18446744073709551616.0"), -18446744073709551616.0f),

            new TestablePair("9223372036854775808", 9223372036854775808.0f),
            new TestablePair("-9223372036854775808", -9223372036854775808.0f),
        };

        testValuesForRegistry(converterRegistry)
            .fromValues(pairs)
            .to(Float.class)
            .assertValues();
    }

    @Test
    void testTooLargeNumbersToFloatConversions() {
        Object[] unsupportedValues = {
            // numbers that have more than 24 significant bits
            2147483647, -2147483647,
            9223372036854775807L, -9223372036854775807L,
            new BigInteger("18446744073709551615"), new BigInteger("-18446744073709551615"),
            // more than upper bound of float 2^127 * (2 - 2^-23)
            BigInteger.valueOf(2).pow(128),
            "six"
        };

        testValuesForRegistry(converterRegistry)
            .fromNotConvertibleValues(unsupportedValues)
            .to(Float.class)
            .assertNotConvertible();
    }

    @Test
    void name() {
        converterRegistry.convert(new BigInteger("18446744073709549568"), Double.class);
        converterRegistry.convert(new BigInteger("18446742974197923840"), Float.class);
        converterRegistry.convert(9223371487098961920L, Float.class);
    }

    @Test
    void testIsConvertibleToDouble() {
        testTypesForRegistry(converterRegistry)
            .from(
                Byte.class, Short.class, Integer.class, Long.class,
                Float.class, BigInteger.class, BigDecimal.class, String.class
            )
            .notFrom(Boolean.class, byte[].class)
            .to(Double.class)
            .assertTypes();
    }

    @Test
    void testToDoubleConversions() {
        TestablePair[] pairs = {
            new TestablePair((byte) 127, 127.0), new TestablePair((byte) -128, -128.0),
            new TestablePair((short) 32767, 32767.0), new TestablePair((short) -32768, -32768.0),
            new TestablePair(2147483647, 2147483647.0), new TestablePair(-2147483648, -2147483648.0),

            // max long cannot be accommodated exactly,
            // the last long less than max int being exactly representable is 9223372036854774784
            // (equal to Double.longBitsToDouble(0x43DFFFFFFFFFFFFFL) - max integral double within 2^62..2^63-1 range)
            new TestablePair(9223372036854774784L, 9223372036854774784.0),
            // the pen-ultimate long less than max long being exactly representable is 9223372036854773760
            // (equal to Double.longBitsToDouble(0x43DFFFFFFFFFFFFEL)
            new TestablePair(9223372036854773760L, 9223372036854773760.0),

            new TestablePair(-9223372036854775808L, -9223372036854775808.0),

            new TestablePair(2147483648.0f, 2147483648.0),
            new TestablePair(-2147483648.0f, -2147483648.0),

            new TestablePair(new BigInteger("18446744073709551616"), 18446744073709551616.0),
            new TestablePair(new BigInteger("-18446744073709551616"), -18446744073709551616.0),
            // (equal to Double.longBitsToDouble(0x43EFFFFFFFFFFFFFL) - max integral double within 2^63..2^64-1 range)
            new TestablePair(new BigInteger("18446744073709549568"), 18446744073709549568.0),

            new TestablePair(new BigDecimal("18446744073709551616.0"), 18446744073709551616.0),
            new TestablePair(new BigDecimal("-18446744073709551616.0"), -18446744073709551616.0),
            new TestablePair(new BigDecimal("32767.5"), 32767.5),
            new TestablePair(new BigDecimal("0.0"), 0.0),

            new TestablePair("9223372036854775808", 9223372036854775808.0),
            new TestablePair("-9223372036854775808", -9223372036854775808.0),
            new TestablePair("64.5", 64.5),
        };

        testValuesForRegistry(converterRegistry)
            .fromValues(pairs)
            .to(Double.class)
            .assertValues();
    }

    @Test
    void testUnsupportedToDoubleConversions() {
        Object[] unsupportedValues = {
            // numbers that have more than 53 significant bits
            9223372036854775807L, -9223372036854775807L,
            new BigInteger("18446744073709551615"), new BigInteger("-18446744073709551615"),
            // more than upper bound of double 2^1023 * (2 - 2^-52)
            BigInteger.valueOf(2).pow(1024),
            "six"
        };

        testValuesForRegistry(converterRegistry)
            .fromNotConvertibleValues(unsupportedValues)
            .to(Double.class)
            .assertNotConvertible();
    }

    @Test
    void testIsConvertibleToBoolean() {
        testTypesForRegistry(converterRegistry)
            .from(
                Byte.class, Short.class, Integer.class, Long.class,
                Float.class, Double.class, BigInteger.class, BigDecimal.class, String.class
            )
            .notFrom(byte[].class)
            .to(Boolean.class)
            .assertTypes();
    }

    @Test
    void testToBooleanConversions() {
        TestablePair[] pairs = {
            new TestablePair((byte) 0, false), new TestablePair((byte) 1, true),
            new TestablePair((short) 0, false), new TestablePair((short) 1, true),
            new TestablePair(0, false), new TestablePair(1, true),
            new TestablePair(0L, false), new TestablePair(1L, true),
            new TestablePair(-0.0f, false), new TestablePair(0.0f, false),
            new TestablePair(1.0f, true),
            new TestablePair(-0.0d, false), new TestablePair(0.0d, false),
            new TestablePair(1.0d, true),
            new TestablePair(BigInteger.ZERO, false), new TestablePair(BigInteger.ONE, true),
            new TestablePair(BigDecimal.ZERO, false), new TestablePair(BigDecimal.ONE, true),
            new TestablePair("false", false), new TestablePair("true", true),
            new TestablePair("0", false), new TestablePair("1", true),
        };

        testValuesForRegistry(converterRegistry)
            .fromValues(pairs)
            .to(Boolean.class)
            .assertValues();
    }

    @Test
    void testUnsupportedToBooleanConversions() {
        Object[] unsupportedValues = {
            (byte) 2, 3, 4L,
            1.2f, 0.5f, 6.2f, Float.NEGATIVE_INFINITY,
            7.0d, 0.3d, 1.2d, Double.POSITIVE_INFINITY,
            new BigInteger("8"), new BigDecimal("9.5"),
            "f", "t", "y", "n", "on", "off", "yes", "no", " 1", "0 "
        };

        testValuesForRegistry(converterRegistry)
            .fromNotConvertibleValues(unsupportedValues)
            .to(Boolean.class)
            .assertNotConvertible();
    }

    @Test
    void testIsConvertibleToBigInteger() {
        testTypesForRegistry(converterRegistry)
            .from(
                Byte.class, Short.class, Integer.class, Long.class,
                Float.class, Double.class, BigDecimal.class, String.class
            )
            .notFrom(Boolean.class, byte[].class)
            .to(BigInteger.class)
            .assertTypes();
    }

    @Test
    void testToBigIntegerConversions() {
        TestablePair[] pairs = {
            new TestablePair((byte) -128, BigInteger.valueOf(-128)),
            new TestablePair((byte) 127, BigInteger.valueOf(127)),
            new TestablePair((short) 32767, BigInteger.valueOf(32767)),
            new TestablePair((short) -32768, BigInteger.valueOf(-32768)),
            new TestablePair(2147483647, BigInteger.valueOf(2147483647)),
            new TestablePair(-2147483648, BigInteger.valueOf(-2147483648)),
            new TestablePair(9223372036854775807L, BigInteger.valueOf(9223372036854775807L)),
            new TestablePair(-9223372036854775808L, BigInteger.valueOf(-9223372036854775808L)),
            new TestablePair(-0.0f, BigInteger.ZERO),
            new TestablePair(0.0f, BigInteger.ZERO),
            new TestablePair(18_446_744_073_709_551_616.0f, new BigInteger("18446744073709551616")),
            new TestablePair(-0.0d, BigInteger.ZERO),
            new TestablePair(0.0d, BigInteger.ZERO),
            new TestablePair(18_446_744_073_709_551_616.0d, new BigInteger("18446744073709551616")),
            new TestablePair("18446744073709551615", new BigInteger("18446744073709551615")),
        };

        testValuesForRegistry(converterRegistry)
            .fromValues(pairs)
            .to(BigInteger.class)
            .assertValues();
    }

    @Test
    void testUnsupportedToBigIntegerConversions() {
        Object[] unsupportedValues = {
            65.5f,
            65.5f,
            Float.NaN,
            Float.POSITIVE_INFINITY,
            650.5d,
            Double.NaN,
            Double.NEGATIVE_INFINITY,
            BigDecimal.valueOf(165.5),
            "12.5",
            "four"
        };

        testValuesForRegistry(converterRegistry)
            .fromNotConvertibleValues(unsupportedValues)
            .to(BigInteger.class)
            .assertNotConvertible();
    }

    @Test
    void testIsConvertibleToBigDecimal() {
        testTypesForRegistry(converterRegistry)
            .from(
                Byte.class, Short.class, Integer.class, Long.class,
                Float.class, Double.class, BigInteger.class, String.class
            )
            .notFrom(Boolean.class, byte[].class)
            .to(BigDecimal.class)
            .assertTypes();
    }

    @Test
    void testToBigDecimalConversions() {
        TestablePair[] pairs = {
            new TestablePair((byte) -128, BigDecimal.valueOf(-128)),
            new TestablePair((byte) 127, BigDecimal.valueOf(127)),
            new TestablePair((short) 32767, BigDecimal.valueOf(32767)),
            new TestablePair((short) -32768, BigDecimal.valueOf(-32768)),
            new TestablePair(2147483647, BigDecimal.valueOf(2147483647)),
            new TestablePair(-2147483648, BigDecimal.valueOf(-2147483648)),
            new TestablePair(9223372036854775807L, BigDecimal.valueOf(9223372036854775807L)),
            new TestablePair(-9223372036854775808L, BigDecimal.valueOf(-9223372036854775808L)),
            new TestablePair(12.5f, BigDecimal.valueOf(12.5f)),
            new TestablePair(-650.5d, BigDecimal.valueOf(-650.5d)),
            new TestablePair(18_446_744_073_709_551_616.0f, new BigDecimal("18446744073709551616")),
            new TestablePair(18_446_744_073_709_551_616.0d, new BigDecimal("18446744073709551616")),
            new TestablePair(new BigInteger("18446744073709551616"), new BigDecimal("18446744073709551616")),
            new TestablePair("18446744073709551615", new BigDecimal("18446744073709551615")),
        };

        testValuesForRegistry(converterRegistry)
            .fromValues(pairs)
            .to(BigDecimal.class)
            .assertValues();
    }

    @Test
    void testUnsupportedToBigDecimalConversions() {
        Object[] unsupportedValues = {
            Float.NaN, Float.POSITIVE_INFINITY, Float.NEGATIVE_INFINITY,
            Double.NaN, Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY,
            "five with half", "NaN", "Infinity", "-Infinity"
        };

        testValuesForRegistry(converterRegistry)
            .fromNotConvertibleValues(unsupportedValues)
            .to(BigDecimal.class)
            .assertNotConvertible();
    }

    @Test
    void testIsConvertibleToString() {
        testTypesForRegistry(converterRegistry)
            .from(
                Byte.class, Short.class, Integer.class, Long.class,
                Float.class, Double.class, BigInteger.class, BigDecimal.class,
                Boolean.class, byte[].class
            )
            .to(String.class)
            .assertTypes();
    }

    @Test
    void testToStringConversions() {
        TestablePair[] pairs = {
            new TestablePair((byte) 127, "127"), new TestablePair((byte) -128, "-128"),
            new TestablePair((short) 32767, "32767"), new TestablePair((short) -32768, "-32768"),
            new TestablePair(2147483647, "2147483647"), new TestablePair(-2147483648, "-2147483648"),
            new TestablePair(9223372036854775807L, "9223372036854775807"),
            new TestablePair(-9223372036854775808L, "-9223372036854775808"),

            new TestablePair(0x1.fffffffffffffP+1023, "1.7976931348623157E308"),
            new TestablePair(0x0.0000000000001P-1022, "4.9E-324"),
            new TestablePair(0.001, "0.001"), new TestablePair(0.0001, "1.0E-4"),
            new TestablePair(0.001, "0.001"), new TestablePair(0.0001, "1.0E-4"),
            new TestablePair(Double.NaN, "NaN"), new TestablePair(10_000_000_000_000_000_000.0, "1.0E19"),
            new TestablePair(Double.NEGATIVE_INFINITY, "-Infinity"),
            new TestablePair(Double.POSITIVE_INFINITY, "Infinity"),

            new TestablePair(BigInteger.valueOf(2).pow(64).subtract(BigInteger.ONE), "18446744073709551615"),
            new TestablePair(BigDecimal.valueOf(10_000_000_000_000_000_000.0), "1.0E+19"),
            new TestablePair(BigDecimal.valueOf(0x0.0000000000001P-1022), "4.9E-324"),

            new TestablePair(true, "true"), new TestablePair(false, "false"),
            new TestablePair(
                new byte[] {(byte) 0x67, (byte) 0x61, (byte) 0x72, (byte) 0xc3, (byte) 0xa7, (byte) 0x6f, (byte) 0x6e},
                "garçon"),
        };

        testValuesForRegistry(converterRegistry)
            .fromValues(pairs)
            .to(String.class)
            .assertValues();
    }

    /**
     * Tests invalid UTF-8 byte sequences.
     *
     * @see <a href="https://en.wikipedia.org/wiki/UTF-8#Invalid_byte_sequences">Bad UTF8</a>
     */
    @Test
    void testUnsupportedToStringConversions() {
        Object[] unsupportedValues = {
            // 0xc0 is not a valid UTF-8 byte
            new byte[] { 0x68, (byte) 0xc0, 0x65, 0x6c, 0x6c, 0x6f },
            // an unexpected continuation byte 0xdc (0b11011100) but 0b10xxxxxx is required as the second byte
            new byte[] { (byte) 0xc2, (byte) 0xdc }
        };

        testValuesForRegistry(converterRegistry)
            .fromNotConvertibleValues(unsupportedValues)
            .to(String.class)
            .assertNotConvertible();
    }

    @Test
    void testIsConvertibleToByteArray() {
        testTypesForRegistry(converterRegistry)
            .from(String.class)
            .notFrom(
                Byte.class, Short.class, Integer.class, Long.class,
                Float.class, Double.class, BigInteger.class, BigDecimal.class,
                Boolean.class
            )
            .to(byte[].class)
            .assertTypes();
    }

    @Test
    void testToByteArrayConversions() {
        TestablePair[] pairs = {
            new TestablePair(
                "hello",
                new byte[] { 0x68, 0x65, 0x6c, 0x6c, 0x6f }
            ),
            new TestablePair(
                "привет",
                new byte[] { (byte) 0xd0, (byte) 0xbf, (byte) 0xd1, (byte) 0x80, (byte) 0xd0, (byte) 0xb8, (byte) 0xd0,
                    (byte) 0xb2, (byte) 0xd0, (byte) 0xb5, (byte) 0xd1, (byte) 0x82 }
            ),
            new TestablePair(
                "你好",
                new byte[] { (byte) 0xe4, (byte) 0xbd, (byte) 0xa0, (byte) 0xe5, (byte) 0xa5, (byte) 0xbd }),
            new TestablePair("hello", new byte[] { 0x68, 0x65, 0x6c, 0x6c, 0x6f })
        };

        for (TestablePair pair : pairs) {
            assertArrayEquals((byte[]) pair.output, converterRegistry.convert(pair.input, byte[].class));
        }
    }

    @Test
    void testRemoveConvertible() {
        assertEquals(10, converterRegistry.convert("10", Integer.class));

        converterRegistry.removeConvertible(String.class, Integer.class);
        NotConvertibleValueException error = assertThrows(
            NotConvertibleValueException.class,
            () -> converterRegistry.convert("10", Integer.class)
        );
        assertEquals(String.class, error.getFrom());
        assertEquals(Integer.class, error.getTo());
    }

    @Test
    void testReplaceConvertible() {
        assertEquals(200, converterRegistry.convert("200", Integer.class));

        converterRegistry.addConverter(String.class, Integer.class, String::length);
        assertEquals(3, converterRegistry.convert("200", Integer.class));
    }

    private static class TestablePair {
        public TestablePair(Object input, Object output) {
            this.input = input;
            this.output = output;
        }

        Object input;
        Object output;
    }

    static class ConversionValuesTester {
        private final ConverterRegistry registry;
        private Class<?> to;
        private TestablePair[] pairs = new TestablePair[0];
        private Object[] ncvObjects = new Object[0];

        static ConversionValuesTester testValuesForRegistry(ConverterRegistry registry) {
            return new ConversionValuesTester(registry);
        }

        ConversionValuesTester(ConverterRegistry registry) {
            this.registry = registry;
        }

        ConversionValuesTester to(Class<?> to) {
            this.to = to;
            return this;
        }

        ConversionValuesTester fromValues(TestablePair... pairs) {
            this.pairs = pairs;
            return this;
        }

        ConversionValuesTester fromNotConvertibleValues(Object... values) {
            this.ncvObjects = values;
            return this;
        }

        void assertValues() {
            for (TestablePair pair : pairs) {
                assertEquals(pair.output, registry.convert(pair.input, to));
            }
        }

        void assertNotConvertible() {
            for (Object object : ncvObjects) {
                assertThrows(NotConvertibleValueException.class, () -> registry.convert(object, to));
            }
        }
    }

    static class ConversionTypesTester {
        private final ConverterRegistry registry;
        private Class<?> to;
        private Class<?>[] convertibleSources = new Class<?>[0];
        private Class<?>[] unconvertibleSources = new Class<?>[0];

        static ConversionTypesTester testTypesForRegistry(ConverterRegistry registry) {
            return new ConversionTypesTester(registry);
        }

        ConversionTypesTester(ConverterRegistry registry) {
            this.registry = registry;
        }

        ConversionTypesTester to(Class<?> to) {
            this.to = to;
            return this;
        }

        ConversionTypesTester from(Class<?>... from) {
            this.convertibleSources = from;
            return this;
        }

        ConversionTypesTester notFrom(Class<?>... from) {
            this.unconvertibleSources = from;
            return this;
        }

        void assertTypes() {
            for (Class<?> from : convertibleSources) {
                assertTrue(registry.isConvertible(from, to));
            }
            for (Class<?> from : unconvertibleSources) {
                assertFalse(registry.isConvertible(from, to));
            }
        }
    }
}
