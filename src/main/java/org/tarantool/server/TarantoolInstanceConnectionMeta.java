package org.tarantool.server;

public class TarantoolInstanceConnectionMeta {

    private final String salt;
    private final String serverVersion;

    public TarantoolInstanceConnectionMeta(String salt, String serverVersion) {
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
