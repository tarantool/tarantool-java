package org.tarantool;

import java.util.ArrayList;
import java.util.List;

/**
 * Tarantool row wrapper. Can be used to obtain elements via
 * 1-based index.
 *
 * <p>
 * The theoretical tuple size is 2^31-1 elements. Thus, it leads
 * a client can request a too large tuple filled by nulls. To handle
 * this case in a less expensive way, this container provides a getting
 * such nulls without an extra memory allocation till the user request
 * via {@link #toList()} or {@link #toArray()}.
 *
 * <p>
 * XXX: consider a implementation of {@link java.util.List} instead
 * (see examples in {@link java.util.Collections}). This will allow
 * to work with the tuple through {@code List} interface directly.
 */
public class TarantoolTuple {
    private final List<Object> source;
    private final int capacity;

    public TarantoolTuple(List<Object> source, int capacity) {
        if (source.size() > capacity) {
            throw new IllegalArgumentException("Tuple capacity is less than source list");
        }
        this.source = source;
        this.capacity = capacity;
    }

    public Object get(int index) {
        if (index < 1 || index > capacity) {
            throw new IndexOutOfBoundsException("Index out of range: " + index);
        }
        if (index > source.size()) {
            return null;
        }
        return source.get(index - 1);
    }

    public int size() {
        return capacity;
    }

    public List<Object> toList() {
        ArrayList<Object> list = new ArrayList<>(capacity);
        list.addAll(source);
        for (int i = list.size(); i < capacity; i++) {
            list.add(null);
        }
        return list;
    }

    public Object[] toArray() {
        Object[] array = new Object[capacity];
        for (int i = 0; i < source.size(); i++) {
            array[i] = source.get(i);
        }
        return array;
    }
}
