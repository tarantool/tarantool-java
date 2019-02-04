package org.tarantool.server;

import java.util.Map;

public class TarantoolBinaryPackage {
    private final Map<Integer, Object> headers;
    private final Map<Integer, Object> body;

    public TarantoolBinaryPackage(Map<Integer, Object> headers, Map<Integer, Object> body) {
        this.headers = headers;
        this.body = body;
    }

    public TarantoolBinaryPackage(Map<Integer, Object> headers) {
        this.headers = headers;
        body = null;
    }

    public Map<Integer, Object> getHeaders() {
        return headers;
    }

    public Map<Integer, Object> getBody() {
        return body;
    }
}
