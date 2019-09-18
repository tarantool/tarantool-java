package org.tarantool;

import static org.tarantool.protocol.ProtoConstants.ERR_LOADING;
import static org.tarantool.protocol.ProtoConstants.ERR_LOCAL_INSTANCE_ID_IS_READ_ONLY;
import static org.tarantool.protocol.ProtoConstants.ERR_READONLY;
import static org.tarantool.protocol.ProtoConstants.ERR_TIMEOUT;
import static org.tarantool.protocol.ProtoConstants.ERR_WRONG_SCHEMA_VERSION;

/**
 * A remote server error with error code and message.
 *
 * @author dgreen
 * @version $Id: $
 */
public class TarantoolException extends RuntimeException {

    private static final long serialVersionUID = 1L;
    long code;

    /**
     * Getter for the field <code>code</code>.
     *
     * @return error code
     */
    public long getCode() {
        return code;
    }

    /**
     * Constructor for TarantoolException.
     *
     * @param code    a int.
     * @param message a {@link java.lang.String} object.
     * @param cause   a {@link java.lang.Throwable} object.
     */
    public TarantoolException(long code, String message, Throwable cause) {
        super(message, cause);
        this.code = code;

    }

    /**
     * Constructor for TarantoolException.
     *
     * @param code    a int.
     * @param message a {@link java.lang.String} object.
     */
    public TarantoolException(long code, String message) {
        super(message);
        this.code = code;

    }

    /**
     * Determines whether this error was caused under transient
     * circumstances or not.
     *
     * @return {@code true} if retry can possibly help to overcome this error.
     */
    public boolean isTransient() {
        switch ((int) code) {
        case ERR_READONLY:
        case ERR_TIMEOUT:
        case ERR_WRONG_SCHEMA_VERSION:
        case ERR_LOADING:
        case ERR_LOCAL_INSTANCE_ID_IS_READ_ONLY:
            return true;
        default:
            return false;
        }
    }
}
