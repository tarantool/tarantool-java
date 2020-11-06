package org.tarantool.dsl;

import org.tarantool.Code;

import java.util.Objects;

/**
 * Supports space related DSL builders.
 *
 * @param <B> current build type
 */
public abstract class SpaceRequestSpec<B extends SpaceRequestSpec<B>>
    extends AbstractRequestSpec<B> {

    Integer spaceId;
    String spaceName;

    public SpaceRequestSpec(Code code) {
        super(code);
    }

    public SpaceRequestSpec(Code code, int spaceId) {
        this(code);
        this.spaceId = spaceId;
    }

    public SpaceRequestSpec(Code code, String spaceName) {
        this(code);
        this.spaceName = Objects.requireNonNull(spaceName);
    }

    @SuppressWarnings("unchecked")
    public B space(int spaceId) {
        this.spaceId = spaceId;
        this.spaceName = null;
        return (B) this;
    }

    @SuppressWarnings("unchecked")
    public B space(String spaceName) {
        this.spaceName = Objects.requireNonNull(spaceName);
        this.spaceId = null;
        return (B) this;
    }

}
