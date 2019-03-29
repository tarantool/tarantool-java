package org.tarantool;

import org.tarantool.protocol.TarantoolPacket;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public abstract class SqlProtoUtils {
    public static List<Map<String, Object>> readSqlResult(TarantoolPacket pack) {
        List<List<?>> data = (List<List<?>>) pack.getBody().get(Key.DATA.getId());

        List<Map<String, Object>> values = new ArrayList<>(data.size());
        List<TarantoolBase.SQLMetaData> metaData = getSQLMetadata(pack);
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

    public static List<TarantoolBase.SQLMetaData> getSQLMetadata(TarantoolPacket pack) {
        List<Map<Integer, Object>> meta = (List<Map<Integer, Object>>) pack.getBody().get(Key.SQL_METADATA.getId());
        List<TarantoolBase.SQLMetaData> values = new ArrayList<TarantoolBase.SQLMetaData>(meta.size());
        for (Map<Integer, Object> c : meta) {
            values.add(new TarantoolBase.SQLMetaData((String) c.get(Key.SQL_FIELD_NAME.getId())));
        }
        return values;
    }

    public static Long getSqlRowCount(TarantoolPacket pack) {
        Map<Key, Object> info = (Map<Key, Object>) pack.getBody().get(Key.SQL_INFO.getId());
        Number rowCount;
        if (info != null && (rowCount = ((Number) info.get(Key.SQL_ROW_COUNT.getId()))) != null) {
            return rowCount.longValue();
        }
        return null;
    }
}
