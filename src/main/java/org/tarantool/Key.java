package org.tarantool;


import java.util.concurrent.Callable;

public enum Key implements Callable<Integer> {
    //header
    CODE(0x00), SYNC(0x01),  SCHEMA_ID(0x05),

    //body
    SPACE(0x10), INDEX(0x11),
    LIMIT(0x12), OFFSET(0x13),
    ITERATOR(0x14),

    KEY(0x20),
    TUPLE(0x21), FUNCTION(0x22),
    USER_NAME(0x23),EXPRESSION(0x27),
    UPSERT_OPS(0x28),
    DATA(0x30), ERROR(0x31),

    SQL_FIELD_NAME(0),
    SQL_METADATA(0x32),
    SQL_TEXT(0x40),
    SQL_BIND(0x41),
    SQL_OPTIONS(0x42),
    SQL_INFO(0x42),
    SQL_ROW_COUNT(0);

    int id;

    Key(int id) {
        this.id = id;
    }

    public int getId() {
        return id;
    }

    public static Key getById(int id) {
        for (Key key : Key.values()) {
            if (id == key.id) {
                return key;
            }
        }
        return null;
    }


    @Override
    public Integer call() throws Exception {
        return id;
    }
}
