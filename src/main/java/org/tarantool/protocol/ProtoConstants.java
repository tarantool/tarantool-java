package org.tarantool.protocol;

public class ProtoConstants {

    private ProtoConstants() {
    }

    public static final long ERROR_TYPE_MARKER = 0x8000;

    public static final long SUCCESS = 0x0;

    /* taken from src/box/errcode.h */
    public static final int ERR_READONLY = 7;
    public static final int ERR_TIMEOUT = 78;
    public static final int ERR_WRONG_SCHEMA_VERSION = 109;
    public static final int ERR_LOADING = 116;
    public static final int ERR_LOCAL_INSTANCE_ID_IS_READ_ONLY = 128;

}
