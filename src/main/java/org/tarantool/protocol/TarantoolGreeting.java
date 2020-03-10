package org.tarantool.protocol;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class TarantoolGreeting {

    private static final Pattern GREETING_LINE =
        Pattern.compile("Tarantool\\s+(?<version>[-.0-9a-g]+)\\s+\\((?<protocol>.*)\\)\\s+(?<uuid>[-0-9a-f]*)");

    private final String serverVersion;
    private final String protocolType;
    private final String instanceUuid;

    public TarantoolGreeting(String greetingLine) {
        Matcher matcher = GREETING_LINE.matcher(greetingLine);
        if (!matcher.find()) {
            throw new IllegalArgumentException("Welcome message '" + greetingLine + "' is incorrect ");
        }
        serverVersion = matcher.group("version");
        protocolType = matcher.group("protocol");
        instanceUuid = matcher.group("uuid");
    }

    public String getServerVersion() {
        return serverVersion;
    }

    public String getProtocolType() {
        return protocolType;
    }

    public String getInstanceUuid() {
        return instanceUuid;
    }
}
