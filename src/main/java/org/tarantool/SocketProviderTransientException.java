package org.tarantool;

public class SocketProviderTransientException extends RuntimeException {
    private static final long serialVersionUID = -1L;

    public SocketProviderTransientException(String message, Throwable cause) {
        super(message, cause);
    }

}
