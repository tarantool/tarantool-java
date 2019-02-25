package org.tarantool.server;

public class TarantoolNodeConnectionMeta {

    private final String salt;
    private final String serverVersion;

    public TarantoolNodeConnectionMeta(String salt, String serverVersion) {
        this.salt = salt;
        this.serverVersion = serverVersion;
    }

    public String getSalt() {
        return salt;
    }

    public String getServerVersion() {
        return serverVersion;
    }
}
