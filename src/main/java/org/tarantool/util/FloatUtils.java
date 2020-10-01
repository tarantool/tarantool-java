package org.tarantool.util;

import java.math.BigDecimal;
import java.math.BigInteger;

/**
 * Set of convenient methods to work with floats and doubles.
 */
public class FloatUtils {

    /**
     * Max float constant as an integer (2^127 * (2 − 2^-23)).
     */
    public static final BigInteger MAX_BIG_FLOAT = new BigDecimal(Float.MAX_VALUE).toBigInteger();

    /**
     * Minimal long value as a exact double.
     */
    public static double MIN_LONG_DOUBLE = (double) Long.MIN_VALUE;

    /**
     * Max double constant as an integer (2^1023 * (2 − 2^-52)).
     */
    public static BigInteger MAX_BIG_DOUBLE = new BigDecimal(Double.MAX_VALUE).toBigInteger();

    /**
     * Missed public double constants from {@link Double}.
     */
    public static final int DOUBLE_SIGNIFICAND_WIDTH = 52;
    public static final long DOUBLE_SIGNIFICAND_MASK = 0x000FFFFFFFFFFFFFL;
    public static final long DOUBLE_SIGNIFICAND_HIDDEN_BIT = 0x0010000000000000L;

    /**
     * Missed public double constants from {@link Float}.
     */
    public static final int FLOAT_SIGNIFICAND_WIDTH = 23;

    /**
     * Checks whether a long is precisely representable as a float.
     *
     * @param value big integer value to be checked
     *
     * @return {@literal true} if the given long can be exactly stored as a float
     */
    public static boolean isFloatExact(long value) {
        value = Math.abs(value);
        int insignificantBitsCount = Long.numberOfTrailingZeros(value) + Long.numberOfLeadingZeros(value);
        // +32 because of a long extension part
        return insignificantBitsCount >= (31 - FLOAT_SIGNIFICAND_WIDTH + 32);
    }

    /**
     * Checks whether a long is precisely representable as a double.
     *
     * @param value big integer value to be checked
     *
     * @return {@literal true} if the given long can be exactly stored as a double
     */
    public static boolean isDoubleExact(long value) {
        value = Math.abs(value);
        int insignificantBitsCount = Long.numberOfTrailingZeros(value) + Long.numberOfLeadingZeros(value);
        return insignificantBitsCount >= (63 - DOUBLE_SIGNIFICAND_WIDTH);
    }

    /**
     * Checks whether a big integer is precisely representable as a float.
     *
     * @param value big integer value to be checked
     *
     * @return {@literal true} if the given big integer can be exactly stored as a float
     */
    public static boolean isBigFloatExact(BigInteger value) {
        value = value.abs();
        if (value.abs().compareTo(MAX_BIG_FLOAT) > 0) {
            return false;
        }
        return (value.bitLength() - value.getLowestSetBit()) <= (FLOAT_SIGNIFICAND_WIDTH + 1);
    }

    /**
     * Checks whether a big integer is precisely representable as a double.
     *
     * @param value big integer value to be checked
     *
     * @return {@literal true} if the given big integer can be exactly stored as a double
     */
    public static boolean isBigDoubleExact(BigInteger value) {
        value = value.abs();
        if (value.compareTo(MAX_BIG_DOUBLE) > 0) {
            return false;
        }
        return (value.bitLength() - value.getLowestSetBit()) <= (DOUBLE_SIGNIFICAND_WIDTH + 1);
    }

    /**
     * Checks whether a double can be casted to {@literal long} without
     * loss of data.
     *
     * <p>
     * It is mostly inspired by Google Guava {@code DoubleMath.isMathematicalInteger}
     * but it does not take into account integral values being out of {@literal long}
     * range.
     *
     * @param value target value to be checked
     *
     * @return {@literal true} if the given double represents an exact long value
     */
    public static boolean isLongExact(double value) {
        if (value == MIN_LONG_DOUBLE || value == 0.0) {
            return true;
        }
        int exp = Math.getExponent(value);
        return exp < 63 && DOUBLE_SIGNIFICAND_WIDTH - Long.numberOfTrailingZeros(getSignificand(value)) <= exp;
    }

    /**
     * Extracts double significand according to IEEE 754.
     * It's also includes an applying of an implicit bit.
     *
     * @param value finite double value
     *
     * @return double significand as a long value
     *
     * @see Double#longBitsToDouble(long)
     */
    private static long getSignificand(double value) {
        int exponent = Math.getExponent(value);
        long doubleBits = Double.doubleToRawLongBits(value) & DOUBLE_SIGNIFICAND_MASK;
        return (exponent == Double.MIN_EXPONENT - 1) ? (doubleBits << 1) : (doubleBits | DOUBLE_SIGNIFICAND_HIDDEN_BIT);
    }

}
