package org.tarantool.dsl;

import java.util.stream.Stream;

public enum Operator {
    ADDITION("+"),
    SUBTRACTION("-"),
    BITWISE_AND("&"),
    BITWISE_OR("|"),
    BITWISE_XOR("^"),
    SPLICE(":"),
    INSERT("!"),
    DELETE("#"),
    ASSIGN("=");

    private final String opCode;

    Operator(String opCode) {
        this.opCode = opCode;
    }

    public String getOpCode() {
        return opCode;
    }

    public static Operator byOpCode(String opCode) {
        return Stream.of(Operator.values())
            .filter(s -> s.getOpCode().equals(opCode))
            .findFirst()
            .orElseThrow(IllegalArgumentException::new);
    }

}
