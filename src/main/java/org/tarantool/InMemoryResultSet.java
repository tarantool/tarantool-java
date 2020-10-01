package org.tarantool;

import org.tarantool.conversion.ConverterRegistry;
import org.tarantool.conversion.NotConvertibleValueException;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Simple implementation of {@link TarantoolResultSet}
 * that contains all tuples in local memory.
 */
class InMemoryResultSet implements TarantoolResultSet {

    private final ConverterRegistry converterRegistry;
    private final List<Object> results;

    private int currentIndex;
    private List<Object> currentTuple;

    InMemoryResultSet(List<Object> rawResult, boolean asSingleResult, ConverterRegistry converterRegistry) {
        currentIndex = -1;
        this.converterRegistry = converterRegistry;

        results = new ArrayList<>();
        ArrayList<Object> copiedResult = new ArrayList<>(rawResult);
        if (asSingleResult) {
            results.add(copiedResult);
        } else {
            results.addAll(copiedResult);
        }
    }

    @Override
    public boolean next() {
        if ((currentIndex + 1) < results.size()) {
            currentTuple = getAsTuple(++currentIndex);
            return true;
        }
        return false;
    }

    @Override
    public boolean previous() {
        if ((currentIndex - 1) >= 0) {
            currentTuple = getAsTuple(--currentIndex);
            return true;
        }
        return false;
    }

    @Override
    public byte getByte(int columnIndex) {
        return getTypedValue(columnIndex, Byte.class, (byte) 0);
    }

    @Override
    public short getShort(int columnIndex) {
        return getTypedValue(columnIndex, Short.class, (short) 0);
    }

    @Override
    public int getInt(int columnIndex) {
        return getTypedValue(columnIndex, Integer.class, 0);
    }

    @Override
    public long getLong(int columnIndex) {
        return getTypedValue(columnIndex, Long.class, 0L);
    }

    @Override
    public float getFloat(int columnIndex) {
        return getTypedValue(columnIndex, Float.class, 0.0f);
    }

    @Override
    public double getDouble(int columnIndex) {
        return getTypedValue(columnIndex, Double.class, 0.0d);
    }

    @Override
    public boolean getBoolean(int columnIndex) {
        return getTypedValue(columnIndex, Boolean.class, false);
    }

    @Override
    public byte[] getBytes(int columnIndex) {
        return getTypedValue(columnIndex, byte[].class, null);
    }

    @Override
    public String getString(int columnIndex) {
        return getTypedValue(columnIndex, String.class, null);
    }

    @Override
    public Object getObject(int columnIndex) {
        return requireInRange(columnIndex);
    }

    @Override
    public BigInteger getBigInteger(int columnIndex) {
        return getTypedValue(columnIndex, BigInteger.class, null);
    }

    @Override
    @SuppressWarnings("unchecked")
    public List<Object> getList(int columnIndex) {
        Object value = requireInRange(columnIndex);
        if (value == null) {
            return null;
        }
        if (value instanceof List<?>) {
            return (List<Object>) value;
        }
        throw new NotConvertibleValueException(value.getClass(), List.class);
    }

    @Override
    @SuppressWarnings("unchecked")
    public Map<Object, Object> getMap(int columnIndex) {
        Object value = requireInRange(columnIndex);;
        if (value == null) {
            return null;
        }
        if (value instanceof Map<?, ?>) {
            return (Map<Object, Object>) value;
        }
        throw new NotConvertibleValueException(value.getClass(), Map.class);
    }

    @Override
    public boolean isNull(int columnIndex) {
        Object value = requireInRange(columnIndex);
        return value == null;
    }

    @Override
    public TarantoolTuple getTuple(int size) {
        requireInRow();
        int capacity = size == 0 ? currentTuple.size() : size;
        return new TarantoolTuple(currentTuple, capacity);
    }

    @Override
    public int getRowSize() {
        return (currentTuple != null) ? currentTuple.size() : -1;
    }

    @Override
    public boolean isEmpty() {
        return results.isEmpty();
    }

    @Override
    public void close() {
        results.clear();
        currentTuple = null;
        currentIndex = -1;
    }

    @SuppressWarnings("unchecked")
    private <R> R getTypedValue(int columnIndex, Class<R> type, R defaultValue) {
        Object value = requireInRange(columnIndex);
        if (value == null) {
            return defaultValue;
        }
        if (type.isInstance(value)) {
            return (R) value;
        }
        return converterRegistry.convert(value, type);
    }

    @SuppressWarnings("unchecked")
    private List<Object> getAsTuple(int index) {
        Object row = results.get(index);
        return (List<Object>) row;
    }

    private Object requireInRange(int index) {
        requireInRow();
        if (index < 1 || index > currentTuple.size()) {
            throw new IndexOutOfBoundsException("Index out of range: " + index);
        }
        return currentTuple.get(index - 1);
    }

    private void requireInRow() {
        if (currentIndex == -1) {
            throw new IllegalArgumentException("Result set out of row position. Try call next() before.");
        }
    }

}
