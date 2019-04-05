package org.tarantool;

import org.tarantool.protocol.TarantoolPacket;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

@Deprecated
public class JDBCBridge {

    public static final JDBCBridge EMPTY = new JDBCBridge(Collections.emptyList(), Collections.emptyList());

    final List<SqlProtoUtils.SQLMetaData> sqlMetadata;
    final List<List<Object>> rows;

    protected JDBCBridge(TarantoolPacket pack) {
        this(SqlProtoUtils.getSQLMetadata(pack), SqlProtoUtils.getSQLData(pack));
    }

    protected JDBCBridge(List<SqlProtoUtils.SQLMetaData> sqlMetadata, List<List<Object>> rows) {
        this.sqlMetadata = sqlMetadata;
        this.rows = rows;
    }

    public static JDBCBridge query(TarantoolConnection connection, String sql, Object... params) {
        TarantoolPacket pack = connection.sql(sql, params);
        return new JDBCBridge(pack);
    }

    public static int update(TarantoolConnection connection, String sql, Object... params) {
        return connection.update(sql, params).intValue();
    }

    /**
     * Constructs a JDBCBridge with a predefined data.
     *
     * @param fields fields metadata
     * @param values tuples
     *
     * @return bridge
     */
    public static JDBCBridge mock(List<String> fields, List<List<Object>> values) {
        List<SqlProtoUtils.SQLMetaData> meta = new ArrayList<>(fields.size());
        for (String field : fields) {
            meta.add(new SqlProtoUtils.SQLMetaData(field));
        }
        return new JDBCBridge(meta, values);
    }

    /**
     * Constructs a JDBCBridge with a parsed query result.
     *
     * @param connection connection to be used
     * @param sql        query string
     * @param params     query binding parameters
     *
     * @return bridge
     */
    public static Object execute(TarantoolConnection connection, String sql, Object... params) {
        TarantoolPacket pack = connection.sql(sql, params);
        Long rowCount = SqlProtoUtils.getSqlRowCount(pack);
        if (rowCount == null) {
            return new JDBCBridge(pack);
        }
        return rowCount.intValue();
    }

    public List<List<Object>> getRows() {
        return rows;
    }

    public List<SqlProtoUtils.SQLMetaData> getSqlMetadata() {
        return sqlMetadata;
    }

    @Override
    public String toString() {
        return "JDBCBridge{" +
            "sqlMetadata=" + sqlMetadata +
            ", rows=" + rows +
            '}';
    }

}
