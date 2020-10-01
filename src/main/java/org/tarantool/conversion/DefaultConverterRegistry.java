package org.tarantool.conversion;

import org.tarantool.util.FloatUtils;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Registers default converters between types than can be
 * implicitly converted using, for example, in {@link org.tarantool.TarantoolResultSet}.
 *
 * <p>
 * <table>
 * <caption>Built-in supported convertible pairs</caption>
 *  <thead>
 *  <tr>
 *    <th>↓ from \ to →</th>
 *    <th>byte</th> <th>short</th> <th>int</th>
 *    <th>long</th> <th>float</th> <th>double</th>
 *    <th>boolean</th> <th>BigInteger</th> <th>BigDecimal</th>
 *    <th>String</th> <th>byte[]</th>
 *  </tr>
 *  </thead>
 *  <tbody>
 *  <tr align="center">
 *    <th scope="row">byte</th>
 *    <td>x</td> <td>+</td> <td>+</td>
 *    <td>+</td> <td>+</td> <td>+</td>
 *    <td>-/+</td> <td>+</td> <td>+</td>
 *    <td>+</td> <td>-</td>
 *  </tr>
 *  <tr align="center">
 *    <th scope="row">short</th>
 *    <td>-/+</td> <td>x</td> <td>+</td>
 *    <td>+</td> <td>+</td> <td>+</td>
 *    <td>-/+</td> <td>+</td> <td>+</td>
 *    <td>+</td> <td>-</td>
 *  </tr>
 *  <tr align="center">
 *    <th scope="row">int</th>
 *    <td>-/+</td> <td>-/+</td> <td>x</td>
 *    <td>+</td> <td>-/+</td> <td>+</td>
 *    <td>-/+</td> <td>+</td> <td>+</td>
 *    <td>+</td> <td>-</td>
 *  </tr>
 *  <tr align="center">
 *    <th scope="row">long</th>
 *    <td>-/+</td> <td>-/+</td> <td>-/+</td>
 *    <td>x</td> <td>-/+</td> <td>-/+</td>
 *    <td>-/+</td> <td>+</td> <td>+</td>
 *    <td>+</td> <td>-</td>
 *  </tr>
 *  <tr align="center">
 *    <th scope="row">float</th>
 *    <td>-/+</td> <td>-/+</td> <td>-/+</td>
 *    <td>-/+</td> <td>x</td> <td>+/-</td>
 *    <td>-/+</td> <td>-/+</td> <td>-/+</td>
 *    <td>+/-</td> <td>-</td>
 *  </tr>
 *  <tr align="center">
 *    <th scope="row">double</th>
 *    <td>-/+</td> <td>-/+</td> <td>-/+</td>
 *    <td>-/+</td> <td>+/-</td> <td>x</td>
 *    <td>-/+</td> <td>-/+</td> <td>-/+</td>
 *    <td>+/-</td> <td>-</td>
 *  </tr>
 *  <tr align="center">
 *    <th scope="row">boolean</th>
 *    <td>-</td> <td>-</td> <td>-</td>
 *    <td>-</td> <td>-</td> <td>-</td>
 *    <td>x</td> <td>-</td> <td>-</td>
 *    <td>+</td> <td>-</td>
 *  </tr>
 *  <tr align="center">
 *    <th scope="row">BigInteger</th>
 *    <td>-/+</td> <td>-/+</td> <td>-/+</td>
 *    <td>-/+</td> <td>-/+</td> <td>-/+</td>
 *    <td>-/+</td> <td>x</td> <td>+</td>
 *    <td>+</td> <td>-</td>
 *  </tr>
 *  <tr align="center">
 *    <th scope="row">BigDecimal</th>
 *    <td>-/+</td> <td>-/+</td> <td>-/+</td>
 *    <td>-/+</td> <td>+/-</td> <td>+/-</td>
 *    <td>-/+</td> <td>-/+</td> <td>x</td>
 *    <td>+/-</td> <td>-</td>
 *  </tr>
 *  <tr align="center">
 *    <th scope="row">String</th>
 *    <td>-/+</td> <td>-/+</td> <td>-/+</td>
 *    <td>-/+</td> <td>-/+</td> <td>-/+</td>
 *    <td>-/+</td> <td>-/+</td> <td>-/+</td>
 *    <td>x</td> <td>+</td>
 *  </tr>
 *  <tr align="center">
 *    <th scope="row">byte[]</th>
 *    <td>-</td> <td>-</td> <td>-</td>
 *    <td>-</td> <td>-</td> <td>-</td>
 *    <td>-</td> <td>-</td> <td>-</td>
 *    <td>-/+</td> <td>x</td>
 *  </tr>
 *  </tbody>
 * </table>
 *
 * <p>
 * Where:
 * <ul>
 *     <li>x means not applicable conversion</li>
 *     <li>- means not supported conversion</li>
 *     <li>+ means supported without loss of info</li>
 *     <li>
 *         +/- means supported with possible loss of precision
 *         that will be silently ignored
 *     </li>
 *     <li>
 *         -/+ means supported with possible loss of precision but
 *         a conversion may raise an exception in the case
 *     </li>
 * </ul>
 *
 * <p>
 * There are a few rules applied while conversions:
 *
 * <ul>
 *     <li>
 *         Conversions between float, double, and BigDecimal types are similar
 *         to the <i>narrowing/widening primitive conversion</i> as defined in
 *         <cite>The JLS</cite> (5.1.2, 5.1.3).
 *     </li>
 *     <li>
 *         Conversions from float, double, BigDecimal to String
 *         may produce inexact result that is depended on {@code toString()}
 *         implementation of the specified classes.
 *     </li>
 *     <li>
 *         Conversion between numbers and {@code boolean} is concluded in
 *         comparisons like {@code number == {0|1}}; ({@code true} if a number is
 *         one and {@code false} if a number is zero).
 *     </li>
 *     <li>
 *         Conversion from string to boolean follows the rule where
 *         {@code "true"} and {@code "false"} strings are converted to {@code true}
 *         and {@code false} boolean values respectively. Any other strings are not
 *         supported.
 *     </li>
 *     <li>
 *         Conversion between {@code String} and {@code byte[]} and vice-versa
 *         is performed using {@literal UTF-8} encoding charset.
 *     </li>
 * </ul>
 *
 * @see org.tarantool.TarantoolResultSet
 */
public class DefaultConverterRegistry implements ConverterRegistry {

    private final Map<ConvertiblePair, Converter<?, ?>> converters = new HashMap<>();

    public DefaultConverterRegistry() {
        registerToByteConverters();
        registerToShortConverters();
        registerToIntConverters();
        registerToLongConverters();

        registerToFloatConverters();
        registerToDoubleConverters();

        registerToBooleanConverters();

        registerToBigIntegerConverters();
        registerToBigDecimalConverters();

        registerToStringConverters();
        registerToByteArrayConverters();
    }

    private void registerToByteConverters() {
        Converter<Number, Byte> defaultNumberToByteConverter =
            number -> this.numberToSafeLong(number, Byte.MIN_VALUE, Byte.MAX_VALUE, "Byte").byteValue();

        addConverter(Short.class, Byte.class, defaultNumberToByteConverter);
        addConverter(Integer.class, Byte.class, defaultNumberToByteConverter);
        addConverter(Long.class, Byte.class, defaultNumberToByteConverter);
        addConverter(Float.class, Byte.class, defaultNumberToByteConverter);
        addConverter(Double.class, Byte.class, defaultNumberToByteConverter);
        addConverter(BigInteger.class, Byte.class, defaultNumberToByteConverter);
        addConverter(BigDecimal.class, Byte.class, defaultNumberToByteConverter);
        addConverter(String.class, Byte.class, Byte::parseByte);
    }

    private void registerToShortConverters() {
        Converter<Number, Short> defaultNumberToShortConverter = number ->
            this.numberToSafeLong(number, Short.MIN_VALUE, Short.MAX_VALUE, "Short").shortValue();

        addConverter(Byte.class, Short.class, defaultNumberToShortConverter);
        addConverter(Integer.class, Short.class, defaultNumberToShortConverter);
        addConverter(Long.class, Short.class, defaultNumberToShortConverter);
        addConverter(Float.class, Short.class, defaultNumberToShortConverter);
        addConverter(Double.class, Short.class, defaultNumberToShortConverter);
        addConverter(BigInteger.class, Short.class, defaultNumberToShortConverter);
        addConverter(BigDecimal.class, Short.class, defaultNumberToShortConverter);
        addConverter(String.class, Short.class, Short::parseShort);
    }

    private void registerToIntConverters() {
        Converter<Number, Integer> defaultNumberToIntegerConverter =
            number -> this.numberToSafeLong(number, Integer.MIN_VALUE, Integer.MAX_VALUE, "Integer").intValue();

        addConverter(Byte.class, Integer.class, defaultNumberToIntegerConverter);
        addConverter(Short.class, Integer.class, defaultNumberToIntegerConverter);
        addConverter(Long.class, Integer.class, defaultNumberToIntegerConverter);
        addConverter(Float.class, Integer.class, defaultNumberToIntegerConverter);
        addConverter(Double.class, Integer.class, defaultNumberToIntegerConverter);
        addConverter(BigInteger.class, Integer.class, defaultNumberToIntegerConverter);
        addConverter(BigDecimal.class, Integer.class, defaultNumberToIntegerConverter);
        addConverter(String.class, Integer.class, Integer::parseInt);
    }

    private void registerToLongConverters() {
        Converter<Number, Long> defaultNumberToLongConverter =
            number -> this.numberToSafeLong(number, Long.MIN_VALUE, Long.MAX_VALUE, "Long");
        addConverter(Byte.class, Long.class, defaultNumberToLongConverter);
        addConverter(Short.class, Long.class, defaultNumberToLongConverter);
        addConverter(Integer.class, Long.class, defaultNumberToLongConverter);
        addConverter(Float.class, Long.class, defaultNumberToLongConverter);
        addConverter(Double.class, Long.class, defaultNumberToLongConverter);
        addConverter(BigInteger.class, Long.class, defaultNumberToLongConverter);
        addConverter(BigDecimal.class, Long.class, defaultNumberToLongConverter);
        addConverter(String.class, Long.class, Long::parseLong);
    }

    private void registerToFloatConverters() {
        Converter<Number, Float> defaultNumberToFloatConverter = Number::floatValue;

        addConverter(Byte.class, Float.class, defaultNumberToFloatConverter);
        addConverter(Short.class, Float.class, defaultNumberToFloatConverter);
        addConverter(Integer.class, Float.class, this::numberToFloat);
        addConverter(Long.class, Float.class, this::numberToFloat);
        addConverter(Double.class, Float.class, defaultNumberToFloatConverter);
        addConverter(BigInteger.class, Float.class, this::numberToFloat);
        addConverter(BigDecimal.class, Float.class, defaultNumberToFloatConverter);
        addConverter(String.class, Float.class, Float::parseFloat);
    }

    private void registerToDoubleConverters() {
        Converter<Number, Double> defaultNumberToDoubleConverter = Number::doubleValue;

        addConverter(Byte.class, Double.class, defaultNumberToDoubleConverter);
        addConverter(Short.class, Double.class, defaultNumberToDoubleConverter);
        addConverter(Integer.class, Double.class, defaultNumberToDoubleConverter);
        addConverter(Long.class, Double.class, this::numberToDouble);
        addConverter(Float.class, Double.class, defaultNumberToDoubleConverter);
        addConverter(BigInteger.class, Double.class, this::numberToDouble);
        addConverter(BigDecimal.class, Double.class, defaultNumberToDoubleConverter);
        addConverter(String.class, Double.class, Double::parseDouble);
    }

    private void registerToBooleanConverters() {
        Converter<Number, Boolean> defaultNumberToBooleanConverter =
            number -> this.numberToSafeLong(number, 0, 1, "Boolean").equals(1L);

        addConverter(Byte.class, Boolean.class, defaultNumberToBooleanConverter);
        addConverter(Short.class, Boolean.class, defaultNumberToBooleanConverter);
        addConverter(Integer.class, Boolean.class, defaultNumberToBooleanConverter);
        addConverter(Long.class, Boolean.class, defaultNumberToBooleanConverter);
        addConverter(Float.class, Boolean.class, defaultNumberToBooleanConverter);
        addConverter(Double.class, Boolean.class, defaultNumberToBooleanConverter);
        addConverter(BigInteger.class, Boolean.class, defaultNumberToBooleanConverter);
        addConverter(BigDecimal.class, Boolean.class, defaultNumberToBooleanConverter);
        addConverter(String.class, Boolean.class, string -> {
            if ("0".equalsIgnoreCase(string) || "false".equalsIgnoreCase(string)) {
                return false;
            }
            if ("1".equalsIgnoreCase(string) || "true".equalsIgnoreCase(string)) {
                return true;
            }
            throw new NotConvertibleValueException(String.class, Boolean.class);
        });
    }

    private void registerToBigIntegerConverters() {
        Converter<Number, BigInteger> defaultLongToIntegerConverter = number -> BigInteger.valueOf(number.longValue());

        addConverter(Byte.class, BigInteger.class, defaultLongToIntegerConverter);
        addConverter(Short.class, BigInteger.class, defaultLongToIntegerConverter);
        addConverter(Integer.class, BigInteger.class, defaultLongToIntegerConverter);
        addConverter(Long.class, BigInteger.class, defaultLongToIntegerConverter);
        addConverter(Float.class, BigInteger.class, number -> new BigDecimal(number).toBigIntegerExact());
        addConverter(Double.class, BigInteger.class, number -> new BigDecimal(number).toBigIntegerExact());
        addConverter(BigDecimal.class, BigInteger.class, BigDecimal::toBigIntegerExact);
        addConverter(String.class, BigInteger.class, string -> new BigDecimal(string).toBigIntegerExact());
    }

    private void registerToBigDecimalConverters() {
        Converter<Number, BigDecimal> defaultLongToDecimalConverter = number -> BigDecimal.valueOf(number.longValue());
        addConverter(Byte.class, BigDecimal.class, defaultLongToDecimalConverter);
        addConverter(Short.class, BigDecimal.class, defaultLongToDecimalConverter);
        addConverter(Integer.class, BigDecimal.class, defaultLongToDecimalConverter);
        addConverter(Long.class, BigDecimal.class, defaultLongToDecimalConverter);

        Converter<Number, BigDecimal> defaultDoubleToDecimalConverter = number -> new BigDecimal(number.doubleValue());
        addConverter(Float.class, BigDecimal.class, defaultDoubleToDecimalConverter);
        addConverter(Double.class, BigDecimal.class, defaultDoubleToDecimalConverter);

        addConverter(BigInteger.class, BigDecimal.class, BigDecimal::new);
        addConverter(String.class, BigDecimal.class, BigDecimal::new);
    }

    private void registerToStringConverters() {
        Converter<Object, String> defaultToStringConverter = Object::toString;

        addConverter(Byte.class, String.class, defaultToStringConverter);
        addConverter(Short.class, String.class, defaultToStringConverter);
        addConverter(Integer.class, String.class, defaultToStringConverter);
        addConverter(Long.class, String.class, defaultToStringConverter);
        addConverter(Float.class, String.class, defaultToStringConverter);
        addConverter(Double.class, String.class, defaultToStringConverter);
        addConverter(BigInteger.class, String.class, defaultToStringConverter);
        addConverter(BigDecimal.class, String.class, defaultToStringConverter);
        addConverter(Boolean.class, String.class, defaultToStringConverter);
        addConverter(byte[].class, String.class, bytes -> {
            try {
                // strict UTF-8 decoding
                return StringDecoder.getDecoder(StandardCharsets.UTF_8).decode(ByteBuffer.wrap(bytes)).toString();
            } catch (CharacterCodingException cce) {
                throw new NotConvertibleValueException(byte[].class, String.class, cce);
            }
        });
    }

    private void registerToByteArrayConverters() {
        addConverter(String.class, byte[].class, string -> string.getBytes(StandardCharsets.UTF_8));
    }

    @Override
    public boolean isConvertible(Class<?> from, Class<?> to) {
        return converters.containsKey(new ConvertiblePair(from, to));
    }

    @Override
    @SuppressWarnings("unchecked")
    public <B> B convert(Object object, Class<B> targetType) {
        Converter<Object, B> converter =
            (Converter<Object, B>) converters.get(new ConvertiblePair(object.getClass(), targetType));
        if (converter == null) {
            throw new NotConvertibleValueException(object.getClass(), targetType);
        }
        try {
            return converter.convert(object);
        } catch (Exception cause) {
            if (cause instanceof NotConvertibleValueException) {
                throw cause;
            }
            throw new NotConvertibleValueException(object.getClass(), targetType, cause);
        }
    }

    @Override
    public <A, B> void addConverter(Class<A> from, Class<B> to, Converter<? super A, ? extends B> converter) {
        converters.put(new ConvertiblePair(from, to), converter);
    }

    @Override
    public boolean removeConvertible(Class<?> from, Class<?> to) {
        return converters.remove(new ConvertiblePair(from, to)) != null;
    }

    /**
     * Performs type conversion from {Code Number} to {@code Long} types and
     * checks possible overflow issues.
     *
     * @param value number to be converted to Long
     * @param min   lower target type bound
     * @param max   upper target type bound
     * @param type  target type name
     *
     * @return converted value as {@code long}
     */
    private Long numberToSafeLong(Number value, long min, long max, String type) {
        long number = 0L;
        try {
            if (value instanceof BigInteger) {
                number = ((BigInteger) value).longValueExact();
            } else if (value instanceof BigDecimal) {
                number = ((BigDecimal) value).longValueExact();
            } else if (value instanceof Float || value instanceof Double) {
                if (!FloatUtils.isLongExact(value.doubleValue())) {
                    throw new ArithmeticException("Not a long value");
                }
                number = value.longValue();
            } else {
                number = value.longValue();
            }
        } catch (ArithmeticException ignored) {
            throwOutOfRangeException(value, min, max, type);
        }
        if (number < min || number > max) {
            throwOutOfRangeException(number, min, max, type);
        }
        return number;
    }

    private Float numberToFloat(Number value) {
        if (FloatUtils.isFloatExact(value.longValue())) {
            return value.floatValue();
        }
        throw new RuntimeException("Not an integral float");
    }

    private Float numberToFloat(BigInteger value) {
        if (FloatUtils.isBigFloatExact(value)) {
            return value.floatValue();
        }
        throw new RuntimeException("Not an integral float");
    }

    private Double numberToDouble(Number value) {
        if (FloatUtils.isDoubleExact(value.longValue())) {
            return value.doubleValue();
        }
        throw new RuntimeException("Not an integral double");
    }

    private Double numberToDouble(BigInteger value) {
        if (FloatUtils.isBigDoubleExact(value)) {
            return value.doubleValue();
        }
        throw new RuntimeException("Not an integral double");
    }

    private void throwOutOfRangeException(Number value, long min, long max, String type) {
        throw new RuntimeException(
            value + " is out of " + type + " range [" + min + ".." + max + "]"
        );
    }

    private static class ConvertiblePair {
        private final Class<?> fromType;
        private final Class<?> toType;

        public ConvertiblePair(Class<?> fromType, Class<?> toType) {
            this.fromType = fromType;
            this.toType = toType;
        }

        @Override
        public boolean equals(Object other) {
            if (this == other) {
                return true;
            }
            if (other == null || getClass() != other.getClass()) {
                return false;
            }
            ConvertiblePair that = (ConvertiblePair) other;
            return Objects.equals(fromType, that.fromType) &&
                Objects.equals(toType, that.toType);
        }

        @Override
        public int hashCode() {
            return Objects.hash(fromType, toType);
        }
    }
}
