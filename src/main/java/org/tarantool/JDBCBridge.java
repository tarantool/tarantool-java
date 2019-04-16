package org.tarantool;

import org.tarantool.protocol.TarantoolPacket;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;

public class JDBCBridge {

    public static final JDBCBridge EMPTY = new JDBCBridge(Collections.emptyList(), Collections.emptyList());

    final List<TarantoolBase.SQLMetaData> sqlMetadata;
    final Map<String, Integer> columnsByName;
    final List<List<Object>> rows;

    protected JDBCBridge(TarantoolPacket pack) {
        this(SqlProtoUtils.getSQLMetadata(pack), SqlProtoUtils.getSQLData(pack));
    }

    protected JDBCBridge(List<TarantoolBase.SQLMetaData> sqlMetadata, List<List<Object>> rows) {
        this.sqlMetadata = sqlMetadata;
        this.rows = rows;
        columnsByName = new LinkedHashMap<String, Integer>((int) Math.ceil(sqlMetadata.size() / 0.75), 0.75f);
        for (int i = 0; i < sqlMetadata.size(); i++) {
            columnsByName.put(sqlMetadata.get(i).getName(), i + 1);
        }
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
        List<TarantoolBase.SQLMetaData> meta = new ArrayList<>(fields.size());
        for (String field : fields) {
            meta.add(new TarantoolBase.SQLMetaData(field));
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

    public String getColumnName(int columnIndex) {
        return columnIndex > sqlMetadata.size() ? null : sqlMetadata.get(columnIndex - 1).getName();
    }

    public Integer getColumnIndex(String columnName) {
        return columnsByName.get(columnName);
    }

    public int getColumnCount() {
        return columnsByName.size();
    }

    public ListIterator<List<Object>> iterator() {
        return rows.listIterator();
    }

    public int size() {
        return rows.size();
    }

    @Override
    public String toString() {
        return "JDBCBridge{" +
            "sqlMetadata=" + sqlMetadata +
            ", columnsByName=" + columnsByName +
            ", rows=" + rows +
            '}';
    }

}
