package org.tarantool.jdbc;

import org.tarantool.SqlProtoUtils;

import java.util.Collections;
import java.util.List;

/**
 * Union wrapper for SQL query results as well as
 * SQL update results.
 */
public class SQLResultHolder {

    public static final int NO_UPDATE_COUNT = -1;

    private final List<SqlProtoUtils.SQLMetaData> sqlMetadata;
    private final List<List<Object>> rows;
    private final int updateCount;

    public SQLResultHolder(List<SqlProtoUtils.SQLMetaData> sqlMetadata, List<List<Object>> rows, int updateCount) {
        this.sqlMetadata = sqlMetadata;
        this.rows = rows;
        this.updateCount = updateCount;
    }

    public static SQLResultHolder ofQuery(final List<SqlProtoUtils.SQLMetaData> sqlMetadata,
                                          final List<List<Object>> rows) {
        return new SQLResultHolder(sqlMetadata, rows, NO_UPDATE_COUNT);
    }

    public static SQLResultHolder ofEmptyQuery() {
        return ofQuery(Collections.emptyList(), Collections.emptyList());
    }

    public static SQLResultHolder ofUpdate(int updateCount) {
        return new SQLResultHolder(null, null, updateCount);
    }

    public List<SqlProtoUtils.SQLMetaData> getSqlMetadata() {
        return sqlMetadata;
    }

    public List<List<Object>> getRows() {
        return rows;
    }

    public int getUpdateCount() {
        return updateCount;
    }

    public boolean isQueryResult() {
        return sqlMetadata != null && rows != null;
    }

}
