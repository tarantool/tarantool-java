package org.tarantool.protocol;

import org.tarantool.Key;

import java.util.Map;

public class TarantoolPacket {
    private final Map<Integer, Object> headers;
    private final Map<Integer, Object> body;

    public TarantoolPacket(Map<Integer, Object> headers, Map<Integer, Object> body) {
        this.headers = headers;
        this.body = body;
    }

    public TarantoolPacket(Map<Integer, Object> headers) {
        this.headers = headers;
        body = null;
    }

    public Long getCode() {
        Object potenticalCode = headers.get(Key.CODE.getId());

        if (!(potenticalCode instanceof Long)) {
            //noinspection ConstantConditions
            throw new IllegalStateException(
                "A value contained in the header by key '" + Key.CODE.name() +
                    "' is not instance of Long class: " +
                    potenticalCode != null ? potenticalCode.getClass().toString() : "null"
            );
        }
        Long code = (Long) potenticalCode;

        return code == 0 ? code : ProtoUtils.extractErrorCode(code);
    }

    public Long getSync() {
        return (Long) getHeaders().get(Key.SYNC.getId());
    }

    public Map<Integer, Object> getHeaders() {
        return headers;
    }

    public Map<Integer, Object> getBody() {
        return body;
    }

    public boolean hasBody() {
        return body != null && body.size() > 0;
    }

    public long getSchemaId() {
        return (Long) headers.get(Key.SCHEMA_ID.getId());
    }

    public Object getData() {
        return hasBody() ? body.get(Key.DATA.getId()) : null;
    }

    public Object getError() {
        return hasBody() ? body.get(Key.ERROR.getId()) : null;
    }
}
