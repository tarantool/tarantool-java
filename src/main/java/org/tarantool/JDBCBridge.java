package org.tarantool;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;

public class JDBCBridge {
    final List<TarantoolBase.SQLMetaData> sqlMetadata;
    final Map<String,Integer> columnsByName;
    final List<List<Object>> rows;

    protected JDBCBridge(TarantoolConnection connection) {
        this(connection.getSQLMetadata(),connection.getSQLData());
    }

    protected JDBCBridge(List<TarantoolBase.SQLMetaData> sqlMetadata, List<List<Object>> rows) {
        this.sqlMetadata = sqlMetadata;
        this.rows = rows;
        columnsByName = new LinkedHashMap<String, Integer>((int) Math.ceil(sqlMetadata.size() / 0.75), 0.75f);
        for (int i = 0; i < sqlMetadata.size(); i++) {
            columnsByName.put(sqlMetadata.get(i).getName(), i);
        }
    }

    public static JDBCBridge query(TarantoolConnection connection, String sql, Object ... params) {
        connection.sql(sql, params);
        return new JDBCBridge(connection);
    }

    public static int update(TarantoolConnection connection, String sql, Object ... params) {
        return connection.update(sql, params).intValue();
    }

    public static Object execute(TarantoolConnection connection, String sql, Object ... params) {
        connection.sql(sql, params);
        Long rowCount = connection.getSqlRowCount();
        if(rowCount == null) {
            return new JDBCBridge(connection);
        }
        return rowCount.intValue();
    }


    public String getColumnName(int columnIndex) {
       return sqlMetadata.get(columnIndex).getName();
    }

    public Integer getColumnIndex(String columnName) {
        return columnsByName.get(columnName);
    }

    public int getColumnCount() {
        return sqlMetadata.size();
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
