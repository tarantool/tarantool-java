package org.tarantool;

import org.tarantool.jdbc.type.TarantoolSqlType;
import org.tarantool.jdbc.type.TarantoolType;
import org.tarantool.protocol.TarantoolPacket;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public abstract class SqlProtoUtils {
    public static List<Map<String, Object>> readSqlResult(TarantoolPacket pack) {
        List<List<Object>> data = getSQLData(pack);
        List<SQLMetaData> metaData = getSQLMetadata(pack);

        List<Map<String, Object>> values = new ArrayList<>(data.size());
        for (List row : data) {
            LinkedHashMap<String, Object> value = new LinkedHashMap<>();
            for (int i = 0; i < row.size(); i++) {
                value.put(metaData.get(i).getName(), row.get(i));
            }
            values.add(value);
        }
        return values;
    }

    public static List<List<Object>> getSQLData(TarantoolPacket pack) {
        return (List<List<Object>>) pack.getBody().get(Key.DATA.getId());
    }

    public static List<SQLMetaData> getSQLMetadata(TarantoolPacket pack) {
        List<Map<Integer, Object>> meta = (List<Map<Integer, Object>>) pack.getBody().get(Key.SQL_METADATA.getId());
        List<SQLMetaData> values = new ArrayList<>(meta.size());
        for (Map<Integer, Object> item : meta) {
            values.add(new SQLMetaData(
                (String) item.get(Key.SQL_FIELD_NAME.getId()),
                (String) item.get(Key.SQL_FIELD_TYPE.getId()))
            );
        }
        return values;
    }

    public static Long getSQLRowCount(TarantoolPacket pack) {
        Map<Key, Object> info = (Map<Key, Object>) pack.getBody().get(Key.SQL_INFO.getId());
        Number rowCount;
        if (info != null && (rowCount = ((Number) info.get(Key.SQL_ROW_COUNT.getId()))) != null) {
            return rowCount.longValue();
        }
        return null;
    }

    public static List<Integer> getSQLAutoIncrementIds(TarantoolPacket pack) {
        Map<Key, Object> info = (Map<Key, Object>) pack.getBody().get(Key.SQL_INFO.getId());
        if (info != null) {
            List<Integer> generatedIds = (List<Integer>) info.get(Key.SQL_INFO_AUTOINCREMENT_IDS.getId());
            return generatedIds == null ? Collections.emptyList() : generatedIds;
        }
        return Collections.emptyList();
    }

    public static class SQLMetaData {
        private String name;
        private TarantoolSqlType type;

        /**
         * Constructs new SQL metadata based on a raw Tarantool
         * type.
         *
         * Tarantool returns a raw type instead of SQL one.
         * This leads a type mapping ambiguity between raw and
         * SQL types and a default SQL type will be chosen.
         *
         * @param name column name
         * @param tarantoolType raw Tarantool type name
         *
         * @see TarantoolSqlType#getDefaultSqlType(TarantoolType)
         */
        public SQLMetaData(String name, String tarantoolType) {
            this(
                name,
                TarantoolSqlType.getDefaultSqlType(TarantoolType.of(tarantoolType))
            );
        }

        public SQLMetaData(String name, TarantoolSqlType type) {
            this.name = name;
            this.type = type;
        }

        public String getName() {
            return name;
        }

        public TarantoolSqlType getType() {
            return type;
        }

        @Override
        public String toString() {
            return "SQLMetaData{" +
                "name='" + name + '\'' +
                ", type=" + type +
                '}';
        }

    }
}
