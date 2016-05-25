package org.tarantool;

import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.reflect.Array;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;

/**
 * forked from https://bitbucket.org/sirbrialliance/msgpack-java-lite
 */
public class MsgPackLite {

    public static final int OPTION_UNPACK_RAW_AS_STRING = 0x1;
    public static final int OPTION_UNPACK_RAW_AS_BYTE_BUFFER = 0x2;
    public static final int OPTION_UNPACK_NUMBER_UINT32_AS_INT = 0x4;
    public static final int OPTION_UNPACK_NUMBER_AS_LONG = 0x8;

    protected static final int MAX_4BIT = 0xf;
    protected static final int MAX_5BIT = 0x1f;
    protected static final int MAX_7BIT = 0x7f;
    protected static final int MAX_8BIT = 0xff;
    protected static final int MAX_15BIT = 0x7fff;
    protected static final int MAX_16BIT = 0xffff;
    protected static final int MAX_31BIT = 0x7fffffff;
    protected static final long MAX_32BIT = 0xffffffffL;

    //these values are from http://wiki.msgpack.org/display/MSGPACK/Format+specification
    protected static final byte MP_NULL = (byte) 0xc0;
    protected static final byte MP_FALSE = (byte) 0xc2;
    protected static final byte MP_TRUE = (byte) 0xc3;

    protected static final byte MP_FLOAT = (byte) 0xca;
    protected static final byte MP_DOUBLE = (byte) 0xcb;

    protected static final byte MP_FIXNUM = (byte) 0x00;//last 7 bits is value
    protected static final byte MP_UINT8 = (byte) 0xcc;
    protected static final byte MP_UINT16 = (byte) 0xcd;
    protected static final byte MP_UINT32 = (byte) 0xce;
    protected static final byte MP_UINT64 = (byte) 0xcf;

    protected static final byte MP_NEGATIVE_FIXNUM = (byte) 0xe0;//last 5 bits is value
    protected static final int MP_NEGATIVE_FIXNUM_INT = 0xe0;//  /me wishes for signed numbers.
    protected static final byte MP_INT8 = (byte) 0xd0;
    protected static final byte MP_INT16 = (byte) 0xd1;
    protected static final byte MP_INT32 = (byte) 0xd2;
    protected static final byte MP_INT64 = (byte) 0xd3;

    protected static final byte MP_FIXARRAY = (byte) 0x90;//last 4 bits is size
    protected static final int MP_FIXARRAY_INT = 0x90;
    protected static final byte MP_ARRAY16 = (byte) 0xdc;
    protected static final byte MP_ARRAY32 = (byte) 0xdd;

    protected static final byte MP_FIXMAP = (byte) 0x80;//last 4 bits is size
    protected static final int MP_FIXMAP_INT = 0x80;
    protected static final byte MP_MAP16 = (byte) 0xde;
    protected static final byte MP_MAP32 = (byte) 0xdf;

    protected static final byte MP_FIXRAW = (byte) 0xa0;//last 5 bits is size
    protected static final int MP_FIXRAW_INT = 0xa0;
    protected static final byte MP_RAW8 = (byte) 0xd9;
    protected static final byte MP_RAW16 = (byte) 0xda;
    protected static final byte MP_RAW32 = (byte) 0xdb;

    public static void pack(Object item, OutputStream os) throws IOException {
        DataOutputStream out = (os instanceof DataOutputStream) ? (DataOutputStream) os : new DataOutputStream(os);
        if (item instanceof Callable) {
            try {
                item = ((Callable) item).call();
            } catch (Exception e) {
                throw new IllegalArgumentException(e);
            }
        }
        if (item == null) {
            out.write(MP_NULL);
        } else if (item instanceof Boolean) {
            out.write(((Boolean) item).booleanValue() ? MP_TRUE : MP_FALSE);
        } else if (item instanceof Number || item instanceof Code) {
            if (item instanceof Float) {
                out.write(MP_FLOAT);
                out.writeFloat((Float) item);
            } else if (item instanceof Double) {
                out.write(MP_DOUBLE);
                out.writeDouble((Double) item);
            } else {
                long value = item instanceof Code ? ((Code) item).getId() : ((Number) item).longValue();
                if (value >= 0) {
                    if (value <= MAX_7BIT) {
                        out.write((int) value | MP_FIXNUM);
                    } else if (value <= MAX_8BIT) {
                        out.write(MP_UINT8);
                        out.write((int) value);
                    } else if (value <= MAX_16BIT) {
                        out.write(MP_UINT16);
                        out.writeShort((int) value);
                    } else if (value <= MAX_32BIT) {
                        out.write(MP_UINT32);
                        out.writeInt((int) value);
                    } else {
                        out.write(MP_UINT64);
                        out.writeLong(value);
                    }
                } else {
                    if (value >= -(MAX_5BIT + 1)) {
                        out.write((int) (value & 0xff));
                    } else if (value >= -(MAX_7BIT + 1)) {
                        out.write(MP_INT8);
                        out.write((int) value);
                    } else if (value >= -(MAX_15BIT + 1)) {
                        out.write(MP_INT16);
                        out.writeShort((int) value);
                    } else if (value >= -(MAX_31BIT + 1)) {
                        out.write(MP_INT32);
                        out.writeInt((int) value);
                    } else {
                        out.write(MP_INT64);
                        out.writeLong(value);
                    }
                }
            }
        } else if (item instanceof String || item instanceof byte[] || item instanceof ByteBuffer) {
            byte[] data;
            if (item instanceof String) {
                data = ((String) item).getBytes("UTF-8");
            } else if (item instanceof byte[]) {
                data = (byte[]) item;
            } else {
                ByteBuffer bb = ((ByteBuffer) item);
                if (bb.hasArray()) {
                    data = bb.array();
                } else {
                    data = new byte[bb.capacity()];
                    bb.position();
                    bb.limit(bb.capacity());
                    bb.get(data);
                }
            }

            if (data.length <= MAX_5BIT) {
                out.write(data.length | MP_FIXRAW);
            } else if (data.length <= MAX_8BIT) {
                out.write(MP_RAW8);
                out.writeByte(data.length);
            } else if (data.length <= MAX_16BIT) {
                out.write(MP_RAW16);
                out.writeShort(data.length);
            } else {
                out.write(MP_RAW32);
                out.writeInt(data.length);
            }
            out.write(data);
        } else if (item instanceof List || item.getClass().isArray()) {
            int length = item instanceof List ? ((List) item).size() : Array.getLength(item);
            if (length <= MAX_4BIT) {
                out.write(length | MP_FIXARRAY);
            } else if (length <= MAX_16BIT) {
                out.write(MP_ARRAY16);
                out.writeShort(length);
            } else {
                out.write(MP_ARRAY32);
                out.writeInt(length);
            }
            if (item instanceof List) {
                List list = ((List) item);
                for (Object element : list) {
                    pack(element, out);
                }
            } else {
                for (int i = 0; i < length; i++) {
                    pack(Array.get(item, i), out);
                }
            }
        } else if (item instanceof Map) {
            Map<Object, Object> map = (Map<Object, Object>) item;
            if (map.size() <= MAX_4BIT) {
                out.write(map.size() | MP_FIXMAP);
            } else if (map.size() <= MAX_16BIT) {
                out.write(MP_MAP16);
                out.writeShort(map.size());
            } else {
                out.write(MP_MAP32);
                out.writeInt(map.size());
            }
            for (Map.Entry<Object, Object> kvp : map.entrySet()) {
                pack(kvp.getKey(), out);
                pack(kvp.getValue(), out);
            }
        } else {
            throw new IllegalArgumentException("Cannot msgpack object of type " + item.getClass().getCanonicalName());
        }
    }

    private static Number val(Number n, int options) {
        if ((options & OPTION_UNPACK_NUMBER_AS_LONG) > 0) {
            return n.longValue();
        }
        return n;
    }

    public static Object unpack(InputStream is, int options) throws IOException {
        DataInputStream in = is instanceof DataInputStream ? (DataInputStream) is : new DataInputStream(is);
        int value = in.read();
        if (value < 0) {
            throw new CommunicationException("No more input available when expecting a value");
        }
        switch ((byte) value) {
        case MP_NULL:
            return null;
        case MP_FALSE:
            return false;
        case MP_TRUE:
            return true;
        case MP_FLOAT:
            return in.readFloat();
        case MP_DOUBLE:
            return in.readDouble();
        case MP_UINT8:
            return val(in.read(), options);//read single byte, return as int
        case MP_UINT16:
            return val(in.readShort() & MAX_16BIT, options);//read short, trick Java into treating it as unsigned, return int
        case MP_UINT32:
            long n = in.readInt() & MAX_32BIT;
            return val((options & OPTION_UNPACK_NUMBER_UINT32_AS_INT) > 0 && n <= Integer.MAX_VALUE ? (int) n : n, options);
        case MP_UINT64: {
            long v = in.readLong();
            if (v >= 0) {
                return v;
            } else {
                //this is a little bit more tricky, since we don't have unsigned longs
                byte[] bytes = new byte[]{
                        (byte) ((v >> 24) & 0xff),
                        (byte) ((v >> 16) & 0xff),
                        (byte) ((v >> 8) & 0xff),
                        (byte) (v & 0xff),
                };
                return val(new BigInteger(1, bytes), options);
            }
        }
        case MP_INT8:
            return val((byte) in.read(), options);
        case MP_INT16:
            return val(in.readShort(), options);
        case MP_INT32:
            return val(in.readInt(), options);
        case MP_INT64:
            return val(in.readLong(), options);
        case MP_ARRAY16:
            return unpackList(in.readShort() & MAX_16BIT, in, options);
        case MP_ARRAY32:
            return unpackList(in.readInt(), in, options);
        case MP_MAP16:
            return unpackMap(in.readShort() & MAX_16BIT, in, options);
        case MP_MAP32:
            return unpackMap(in.readInt(), in, options);
        case MP_RAW8:
            return unpackRaw(in.readByte() & MAX_8BIT, in, options);
        case MP_RAW16:
            return unpackRaw(in.readShort() & MAX_16BIT, in, options);
        case MP_RAW32:
            return unpackRaw(in.readInt(), in, options);
        }

        if (value >= MP_NEGATIVE_FIXNUM_INT && value <= MP_NEGATIVE_FIXNUM_INT + MAX_5BIT) {
            return (byte) value;
        } else if (value >= MP_FIXARRAY_INT && value <= MP_FIXARRAY_INT + MAX_4BIT) {
            return unpackList(value - MP_FIXARRAY_INT, in, options);
        } else if (value >= MP_FIXMAP_INT && value <= MP_FIXMAP_INT + MAX_4BIT) {
            return unpackMap(value - MP_FIXMAP_INT, in, options);
        } else if (value >= MP_FIXRAW_INT && value <= MP_FIXRAW_INT + MAX_5BIT) {
            return unpackRaw(value - MP_FIXRAW_INT, in, options);
        } else if (value <= MAX_7BIT) {//MP_FIXNUM - the value is value as an int
            return value;
        } else {
            throw new IllegalArgumentException("Input contains invalid type value " + (byte) value);
        }
    }

    protected static List unpackList(int size, DataInputStream in, int options) throws IOException {
        if (size < 0) {
            throw new IllegalArgumentException("Array to unpack too large for Java (more than 2^31 elements)!");
        }
        List ret = new ArrayList(size);
        for (int i = 0; i < size; ++i) {
            ret.add(unpack(in, options));
        }
        return ret;
    }

    protected static Map unpackMap(int size, DataInputStream in, int options) throws IOException {
        if (size < 0) {
            throw new IllegalArgumentException("Map to unpack too large for Java (more than 2^31 elements)!");
        }
        Map ret = new HashMap(size);
        for (int i = 0; i < size; ++i) {
            Object key = unpack(in, options);
            Object value = unpack(in, options);
            ret.put(key, value);
        }
        return ret;
    }

    protected static Object unpackRaw(int size, DataInputStream in, int options) throws IOException {
        if (size < 0) {
            throw new IllegalArgumentException("byte[] to unpack too large for Java (more than 2^31 elements)!");
        }

        byte[] data = new byte[size];
        in.read(data);

        if ((options & OPTION_UNPACK_RAW_AS_BYTE_BUFFER) != 0) {
            return ByteBuffer.wrap(data);
        } else if ((options & OPTION_UNPACK_RAW_AS_STRING) != 0) {
            return new String(data, "UTF-8");
        } else {
            return data;
        }
    }


}
