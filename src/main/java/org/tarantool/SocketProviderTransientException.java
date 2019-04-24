package org.tarantool;

public class SocketProviderTransientException extends RuntimeException {

    public SocketProviderTransientException(String message, Throwable cause) {
        super(message, cause);
    }

}
