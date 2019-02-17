package org.tarantool.server;

public class TarantoolNodeInfo {

    private final String salt;
    private final String serverVersion;

    public TarantoolNodeInfo(String salt, String serverVersion) {
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
