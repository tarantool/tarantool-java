package org.tarantool.dsl;

import java.util.Arrays;
import java.util.List;

public class Operation {

    private final Operator operator;
    private final List<Object> operands;

    public Operation(Operator operator, Object... operands) {
        this.operator = operator;
        this.operands = Arrays.asList(operands);
    }

    public Operator getOperator() {
        return operator;
    }

    public Object[] toArray() {
        Object[] array = new Object[operands.size() + 1];
        array[0] = operator.getOpCode();
        for (int i = 1; i < array.length; i++) {
            array[i] = operands.get(i - 1);
        }
        return array;
    }

    /**
     * It's used to perform a transformation between raw type
     * and type safe DSL Operation class. This is required
     * because of being compatible with old operations interface
     * and a new DSL approach.
     *
     * This client expects an operation in format of simple
     * array or list like {opCode, args...}. For instance,
     * addition 3 to second field will be {"+", 2, 3}
     *
     * @param operation raw operation
     *
     * @return type safe operation
     */
    public static Operation fromArray(Object operation) {
        try {
            if (operation instanceof Object[]) {
                Object[] opArray = (Object[]) operation;
                String code = opArray[0].toString();
                Object[] args = new Object[opArray.length - 1];
                System.arraycopy(opArray, 1, args, 0, args.length);
                return new Operation(Operator.byOpCode(code), args);
            }
            List<?> opList = (List<?>) operation;
            String code = opList.get(0).toString();
            Object[] args = opList.subList(1, opList.size()).toArray();
            return new Operation(Operator.byOpCode(code), args);
        } catch (Exception cause) {
            throw new IllegalArgumentException(
                "Operation is invalid. Use an array or list as {\"opCode\", args...}. " +
                    "Or use request DSL to build type safe operation.",
                cause
            );
        }
    }

}
