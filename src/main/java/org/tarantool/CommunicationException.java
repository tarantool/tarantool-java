package org.tarantool;

/**
 * CommunicationException class.
 *
 * @author dgreen
 * @version $Id: $
 */
public class CommunicationException extends RuntimeException {

    /**
     * Constructor for CommunicationException.
     *
     * @param message
     *            a {@link java.lang.String} object.
     * @param cause
     *            a {@link java.lang.Throwable} object.
     */
    public CommunicationException(String message, Throwable cause) {
        super(message, cause);
    }

    /**
     * Constructor for CommunicationException.
     *
     * @param message
     *            a {@link java.lang.String} object.
     */
    public CommunicationException(String message) {
        super(message);
    }

    private static final long serialVersionUID = 1L;

}
