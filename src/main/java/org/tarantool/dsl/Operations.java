package org.tarantool.dsl;

import java.util.Objects;

public class Operations {

    public static Operation add(int fieldNumber, long value) {
        return new Operation(Operator.ADDITION, fieldNumber, value);
    }

    public static Operation add(String fieldName, long value) {
        return new Operation(Operator.ADDITION, fieldName, value);
    }

    public static Operation subtract(int fieldNumber, long value) {
        return new Operation(Operator.SUBTRACTION, fieldNumber, value);
    }

    public static Operation subtract(String fieldName, long value) {
        return new Operation(Operator.SUBTRACTION, fieldName, value);
    }

    public static Operation bitwiseAnd(int fieldNumber, long value) {
        return new Operation(Operator.BITWISE_AND, fieldNumber, value);
    }

    public static Operation bitwiseAnd(String fieldName, long value) {
        return new Operation(Operator.BITWISE_AND, fieldName, value);
    }

    public static Operation bitwiseOr(int fieldNumber, long value) {
        return new Operation(Operator.BITWISE_OR, fieldNumber, value);
    }

    public static Operation bitwiseOr(String fieldName, long value) {
        return new Operation(Operator.BITWISE_OR, fieldName, value);
    }

    public static Operation bitwiseXor(int fieldNumber, long value) {
        return new Operation(Operator.BITWISE_XOR, fieldNumber, value);
    }

    public static Operation bitwiseXor(String fieldName, long value) {
        return new Operation(Operator.BITWISE_XOR, fieldName, value);
    }

    public static Operation splice(int fieldNumber, int position, int offset, String substitution) {
        return new Operation(Operator.SPLICE, fieldNumber, position, offset, substitution);
    }

    public static Operation splice(String fieldName, int position, int offset, String substitution) {
        return new Operation(Operator.SPLICE, fieldName, position, offset, substitution);
    }

    public static Operation insert(int fieldNumber, Object value) {
        return new Operation(Operator.INSERT, fieldNumber, value);
    }

    public static Operation insert(String fieldName, Object value) {
        return new Operation(Operator.INSERT, fieldName, value);
    }

    public static Operation delete(int fromField, int length) {
        return new Operation(Operator.DELETE, fromField, length);
    }

    public static Operation delete(String fromField, int length) {
        return new Operation(Operator.DELETE, fromField, length);
    }

    public static Operation assign(int fieldNumber, Object value) {
        return new Operation(Operator.ASSIGN, fieldNumber, value);
    }

    public static Operation assign(String fieldName, Object value) {
        return new Operation(Operator.ASSIGN, fieldName, value);
    }

    private static Operation createOperation(Operator operator, int fieldNumber, Object value) {
        Objects.requireNonNull(value);
        return new Operation(operator, fieldNumber, value);
    }

}
