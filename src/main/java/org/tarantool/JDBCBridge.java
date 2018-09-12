package org.tarantool;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;

import org.tarantool.jdbc.SQLResultSet;

public class JDBCBridge {
    public static final JDBCBridge EMPTY = new JDBCBridge(Collections.<TarantoolBase.SQLMetaData>emptyList(), Collections.<List<Object>>emptyList());

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
            columnsByName.put(sqlMetadata.get(i).getName(), i + 1);
        }
    }

    public static JDBCBridge query(TarantoolConnection connection, String sql, Object ... params) {
        connection.sql(sql, params);
        return new JDBCBridge(connection);
    }

    public static int update(TarantoolConnection connection, String sql, Object ... params) {
        return connection.update(sql, params).intValue();
    }

    public static JDBCBridge mock(List<String> fields, List<List<Object>> values)  {
        List<TarantoolBase.SQLMetaData> meta = new ArrayList<TarantoolBase.SQLMetaData>(fields.size());
        for(String field:fields) {
           meta.add(new TarantoolBase.SQLMetaData(field));
        }
        return new JDBCBridge(meta, values);
    }

    public static Object execute(TarantoolConnection connection, String sql, Object ... params) {
        connection.sql(sql, params);
        Long rowCount = connection.getSqlRowCount();
        if(rowCount == null) {
            return new SQLResultSet(new JDBCBridge(connection));
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
