package org.tarantool.jdbc;

import org.tarantool.MsgPackLite;

import java.io.IOException;
import java.io.OutputStream;
import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;

/**
 * Java SQL {@code msg-pack} extension.
 *
 * Provides extra support for {@code msg-pack} serialization.
 *
 * <p>
 * Additional supported types for serialization:
 *
 * <ul>
 *     <li>{@link java.sql.Date} as a MsgPack int value</li>
 *     <li>{@link java.sql.Time} as a MsgPack int value</li>
 *     <li>{@link java.sql.Timestamp} as a MsgPack int value</li>
 *     <li>{@link java.math.BigDecimal} as a MsgPack string value</li>
 * </ul>
 */
public class SQLMsgPackLite extends MsgPackLite {

    public static final SQLMsgPackLite INSTANCE = new SQLMsgPackLite();

    @Override
    public void pack(Object item, OutputStream output) throws IOException {
        if (item instanceof Date) {
            super.pack(((Date) item).getTime(), output);
        } else if (item instanceof Time) {
            super.pack(((Time) item).getTime(), output);
        } else if (item instanceof Timestamp) {
            super.pack(((Timestamp) item).getTime(), output);
        } else if (item instanceof BigDecimal) {
            super.pack(((BigDecimal) item).toPlainString(), output);
        } else {
            super.pack(item, output);
        }
    }
}
