package org.tarantool.jdbc;

import org.tarantool.MsgPackLite;

import java.io.IOException;
import java.io.OutputStream;
import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;

public class SQLMsgPackLite extends MsgPackLite {

    public static final SQLMsgPackLite INSTANCE = new SQLMsgPackLite();

    @Override
    public void pack(Object item, OutputStream os) throws IOException {
        if (item instanceof Date) {
            super.pack(((Date) item).getTime(), os);
        } else if (item instanceof Time) {
            super.pack(((Time) item).getTime(), os);
        } else if (item instanceof Timestamp) {
            super.pack(((Timestamp) item).getTime(), os);
        } else if (item instanceof BigDecimal) {
            super.pack(((BigDecimal) item).toPlainString(), os);
        } else {
            super.pack(item, os);
        }
    }
}
