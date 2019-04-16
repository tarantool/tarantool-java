package org.tarantool.jdbc;

import static org.tarantool.TestUtils.toHex;

import java.sql.Date;

public class SqlTestUtils {
    public static String getCreateTableSQL(String tableName, TntSqlType[] tntTypes) {
        StringBuilder sb = new StringBuilder("CREATE TABLE ");
        sb.append(tableName);
        sb.append("(KEY INT PRIMARY KEY");
        for (TntSqlType tntType : tntTypes) {
            sb.append(", F");
            sb.append(tntType.ordinal());
            sb.append(" ");
            sb.append(tntType.sqlType);
        }
        sb.append(")");
        return sb.toString();
    }

    public static String getParameterizedInsertSQL(String tableName, TntSqlType[] tntTypes) {
        StringBuilder sb = new StringBuilder("INSERT INTO ");
        sb.append(tableName);
        sb.append("(KEY");
        for (TntSqlType tntType : tntTypes) {
            sb.append(", F");
            sb.append(tntType.ordinal());
        }
        sb.append(") VALUES (");
        sb.append("?"); // KEY value
        for (TntSqlType ignored : tntTypes) {
            sb.append(", ?");
        }
        sb.append(")");
        return sb.toString();
    }

    public static <T> String getInsertSQL(String tableName, int start, TntSqlType[] colTypes, T[] vals) {
        StringBuilder sb = new StringBuilder("INSERT INTO ");
        sb.append(tableName);
        sb.append("(KEY");
        for (TntSqlType tntType : colTypes) {
            sb.append(", F");
            sb.append(tntType.ordinal());
        }
        sb.append(") VALUES ");
        for (int row = 0; row < vals.length; row++) {
            if (row > 0) {
                sb.append(", ");
            }
            sb.append("(");
            sb.append(start + row);

            for (TntSqlType ignored : colTypes) {
                sb.append(", ");
                sb.append(quoteSqlValue(vals[row]));
            }
            sb.append(")");
        }
        return sb.toString();
    }

    public static String getSelectSQL(String tableName, int start, int end, TntSqlType[] tntTypes) {
        StringBuilder sb = new StringBuilder("SELECT KEY");
        for (TntSqlType tntType : tntTypes) {
            sb.append(", F");
            sb.append(tntType.ordinal());
        }
        sb.append(" FROM ");
        sb.append(tableName);
        sb.append(" WHERE KEY >= ");
        sb.append(start);
        sb.append(" AND KEY < ");
        sb.append(end);
        return sb.toString();
    }

    public static String quoteSqlValue(Object val) {
        if (val == null) {
            return "null";
        }

        if (val instanceof Boolean) {
            return (Boolean) val ? "1" : "0";
        }

        if (val instanceof String) {
            return "'" + val.toString() + "'";
        }

        if (val instanceof Date) {
            return "'" + val.toString() + "'";
        }

        if (val instanceof byte[]) {
            return "X'" + toHex((byte[]) val) + "'";
        }

        return val.toString();
    }
}
