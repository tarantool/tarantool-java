package org.tarantool;

/**
 * A remote server error with error code and message.
 * 
 * @author dgreen
 * @version $Id: $
 */
public class TarantoolException extends RuntimeException {
	/* taken from src/box/errcode.h */
	public final static int ERR_READONLY = 7;
	public final static int ERR_TIMEOUT = 78;
	public final static int ERR_LOADING = 116;
	public final static int ERR_LOCAL_INSTANCE_ID_IS_READ_ONLY = 128;

	private static final long serialVersionUID = 1L;
	long code;

	/**
	 * <p>
	 * Getter for the field <code>code</code>.
	 * </p>
	 * 
	 * @return a int.
	 */
	public long getCode() {
		return code;
	}

	/**
	 * <p>
	 * Constructor for TarantoolException.
	 * </p>
	 * 
	 * @param code
	 *            a int.
	 * @param message
	 *            a {@link java.lang.String} object.
	 * @param cause
	 *            a {@link java.lang.Throwable} object.
	 */
	public TarantoolException(long code, String message, Throwable cause) {
		super(message, cause);
		this.code = code;

	}

	/**
	 * <p>
	 * Constructor for TarantoolException.
	 * </p>
	 * 
	 * @param code
	 *            a int.
	 * @param message
	 *            a {@link java.lang.String} object.
	 */
	public TarantoolException(long code, String message) {
		super(message);
		this.code = code;

	}

	/**
	 *
	 * @return {@code true} if retry can possibly help to overcome this error.
	 */
	boolean isTransient() {
		switch ((int)code) {
			case ERR_READONLY:
			case ERR_TIMEOUT:
			case ERR_LOADING:
			case ERR_LOCAL_INSTANCE_ID_IS_READ_ONLY:
				return true;
			default:
				return false;
		}
	}
}
