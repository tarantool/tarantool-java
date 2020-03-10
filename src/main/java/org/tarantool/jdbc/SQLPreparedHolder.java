package org.tarantool.jdbc;

import org.tarantool.SqlProtoUtils;

import java.util.List;

/**
 * A wrapper that is used to hold parameters metadata
 * as well as result set metadata of a server prepared statement.
 * <p>
 * For instance, a DQL statement like {@code SELECT a, b FROM t1 WHERE a > ? AND b = ?}
 * after preparation may have the following structure using YAML notation:
 *
 * <pre>
 * {@code
 * ---
 * - stmt_id: 3209603265
 *   metadata:
 *   - name: A
 *     type: integer
 *   - name: B
 *     type: string
 *   params:
 *   - name: '?'
 *     type: ANY
 *   - name: '?'
 *     type: ANY
 *   param_count: 2
 * ...}
 * </pre>
 *
 * <p>
 * In case of DML statements the {@code metadata part} will be skipped.
 */
public class SQLPreparedHolder {

    private final Long statementId;
    private final List<SqlProtoUtils.SQLMetaData> resultMetadata;
    private final List<SqlProtoUtils.SQLMetaData> paramsMetadata;

    public SQLPreparedHolder(Long statementId,
                             List<SqlProtoUtils.SQLMetaData> resultMetadata,
                             List<SqlProtoUtils.SQLMetaData> paramsMetadata) {
        this.statementId = statementId;
        this.resultMetadata = resultMetadata;
        this.paramsMetadata = paramsMetadata;
    }

    public Long getStatementId() {
        return statementId;
    }

    public List<SqlProtoUtils.SQLMetaData> getResultMetadata() {
        return resultMetadata;
    }

    public List<SqlProtoUtils.SQLMetaData> getParamsMetadata() {
        return paramsMetadata;
    }

    @Override
    public String toString() {
        return "SQLPreparedHolder{" +
            "statementId='" + statementId + '\'' +
            ", resultMetadata=" + resultMetadata +
            ", paramsMetadata=" + paramsMetadata +
            '}';
    }
}
