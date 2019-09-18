package org.tarantool;

/**
 * Holds a request argument value.
 */
public interface TarantoolRequestArgument {

    /**
     * Flag indicating that held value can be
     * represented as bytes supported by iproto.
     *
     * @return {@literal true} if value is {@code iproto} serializable
     */
    boolean isSerializable();

    /**
     * Gets a held value.
     *
     * @return wrapped value
     */
    Object getValue();

}
