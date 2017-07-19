package org.tarantool;


public enum Code  {
    SELECT(1), INSERT(2), REPLACE(3), UPDATE(4),
    DELETE(5), OLD_CALL(6), AUTH(7), EVAL(8), UPSERT(9), CALL(10), EXECUTE(11) , PING(64), SUBSCRIBE(66),;

    int id;

    Code(int id) {
        this.id = id;
    }

    public int getId() {
        return id;
    }
}
