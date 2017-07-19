package org.tarantool.jdbc;

import java.io.IOException;
import java.io.OutputStream;
import java.math.BigDecimal;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Date;

import org.tarantool.MsgPackLite;

public class SQLMsgPackLite extends MsgPackLite {

    public static final SQLMsgPackLite INSTANCE = new SQLMsgPackLite();

    @Override
    public void pack(Object item, OutputStream os) throws IOException {
        if(item instanceof Date) {
            super.pack(((Date)item).getTime(), os);
        } if(item instanceof Time) {
            super.pack(((Time)item).getTime(), os);
        } if(item instanceof Timestamp) {
            super.pack(((Timestamp)item).getTime(), os);
        } if(item instanceof BigDecimal) {
            super.pack(((BigDecimal)item).toPlainString(), os);
        } else {
            super.pack(item, os);
        }
    }
}
