/**
 *  Copyright (c) CJSC "Peter-Service"
 */
package org.tarantool.jsr107;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;

import org.tarantool.Code;
import org.tarantool.MsgPackLite;

/**
 * Extends MsgPack types to be packed and unpacked
 * <p>
 * Date types, BigDecimal and raw Java objects supported here
 * </p>
 *
 * @author Evgeniy Zaikin
 * @see MsgPackLite
 */
public class JSRMsgPackLite extends MsgPackLite {

    public static final JSRMsgPackLite INSTANCE = new JSRMsgPackLite();

    protected void packBin(Object item, OutputStream os) throws IOException {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        try {
          DataOutputStream out = new DataOutputStream(os);
          ObjectOutputStream oos = new ObjectOutputStream(bos);
          oos.writeObject(item);
          bos.flush();
          byte[] data = bos.toByteArray();
          out.write(MP_BIN32);
          out.writeInt(data.length);
          out.write(data);
        } finally {
          try {
            bos.close();
          } catch (IOException e) {
            // eat this up
          }
        }
    }

    @Override
    public void pack(Object item, OutputStream os) throws IOException {
        if (item == null) {
            super.pack(item, os);
        } else if (item instanceof Date) {
            //super.pack(((Date)item).getTime(), os);
            packBin(item, os);
        } else if (item instanceof BigDecimal) {
            //super.pack(((BigDecimal)item).toPlainString(), os);
            packBin(item, os);
        } else if (item instanceof Callable) {
            super.pack(item, os);
        } else if (item instanceof Boolean) {
            super.pack(item, os);
        } else if (item instanceof Number || item instanceof Code) {
            super.pack(item, os);
        } else if (item instanceof String) {
            super.pack(item, os);
        } else if (item instanceof byte[] || item instanceof ByteBuffer) {
            super.pack(item, os);
        } else if (item instanceof List || item.getClass().isArray()) {
            super.pack(item, os);
        } else if (item instanceof Map) {
            super.pack(item, os);
        } else if (item instanceof Callable) {
            super.pack(item, os);
        } else {
            packBin(item, os);
        }
    }

    @Override
    protected Object unpackBin(int size, DataInputStream in) throws IOException {
        if (size < 0) {
            throw new IllegalArgumentException("byte[] to unpack too large for Java (more than 2^31 elements)!");
        }

        byte[] data = new byte[size];
        in.readFully(data);

        ByteArrayInputStream bos = new ByteArrayInputStream(data);
        try (ObjectInputStream ois = new ObjectInputStream(bos)) 
        {
          return ois.readObject();
        } catch (IOException e) {
          throw e;
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } finally {
          try {
            bos.close();
          } catch (IOException e) {
            // eat this up
          }
        }

        return data;
    }
}
