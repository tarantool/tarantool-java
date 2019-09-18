package org.tarantool.schema;

/**
 * Provides Tarantool instance schema info.
 */
public interface TarantoolSchemaMeta {

    /**
     * Finds a space by name if any.
     *
     * @param spaceName name of target space
     *
     * @return found space
     */
    TarantoolSpaceMeta getSpace(String spaceName);

    /**
     * Finds a space index by name if any.
     *
     * @param spaceName name of target space
     * @param indexName name of target index
     *
     * @return found index meta
     */
    TarantoolIndexMeta getSpaceIndex(String spaceName, String indexName);

    /**
     * Gets current schema version that is cached.
     *
     * @return current version
     */
    long getSchemaVersion();

    /**
     * Fetches schema metadata.
     *
     * @return fetched schema metadata version
     */
    long refresh();

    /**
     * Checks whether a schema fully cached or not.
     *
     * @return {@literal true} if the schema is cached at least once
     */
    boolean isInitialized();

}
