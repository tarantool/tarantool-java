package org.tarantool;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

public abstract class TarantoolBase<Result> extends AbstractTarantoolOps<Integer, List<?>, Object, Result> {
    /**
     * Connection state
     */
    protected String salt;
    protected MsgPackLite msgPackLite = MsgPackLite.INSTANCE;
    protected AtomicLong syncId = new AtomicLong();
    protected int initialRequestSize = 4096;
    /**
     * Read properties
     */
    protected DataInputStream is;
    protected CountInputStream cis;
    protected Map<Integer, Object> headers;
    protected Map<Integer, Object> body;

    public TarantoolBase() {
    }

    public TarantoolBase(String username, String password, Socket socket) {
        super();
        try {
            this.is = new DataInputStream(cis = new CountInputStreamImpl(socket.getInputStream()));
            byte[] bytes = new byte[64];
            is.readFully(bytes);
            String firstLine = new String(bytes);
            if (!firstLine.startsWith("Tarantool")) {
                close();
                throw new CommunicationException("Welcome message should starts with tarantool but starts with '" + firstLine + "'", new IllegalStateException("Invalid welcome packet"));
            }
            is.readFully(bytes);
            this.salt = new String(bytes);
            if (username != null && password != null) {
                ByteBuffer authPacket = createAuthPacket(username, password);
                OutputStream os = socket.getOutputStream();
                os.write(authPacket.array(), 0, authPacket.remaining());
                os.flush();
                readPacket(is);
                Long code = (Long) headers.get(Key.CODE.getId());
                if (code != 0) {
                    throw serverError(code, body.get(Key.ERROR.getId()));
                }
            }
        } catch (IOException e) {
            try {
                is.close();
            } catch (IOException ignored) {

            }
            try {
                cis.close();
            } catch (IOException ignored) {

            }
            throw new CommunicationException("Couldn't connect to tarantool", e);
        }
    }


    protected ByteBuffer createAuthPacket(String username, final String password) throws IOException {
        final MessageDigest sha1;
        try {
            sha1 = MessageDigest.getInstance("SHA-1");
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException(e);
        }
        List auth = new ArrayList(2);
        auth.add("chap-sha1");

        byte[] p = sha1.digest(password.getBytes());

        sha1.reset();
        byte[] p2 = sha1.digest(p);

        sha1.reset();
        sha1.update(Base64.decode(salt), 0, 20);
        sha1.update(p2);
        byte[] scramble = sha1.digest();
        for (int i = 0, e = 20; i < e; i++) {
            p[i] ^= scramble[i];
        }
        auth.add(p);
        return createPacket(Code.AUTH, 0L, null, Key.USER_NAME, username, Key.TUPLE, auth);
    }

    protected ByteBuffer createPacket(Code code, Long syncId, Long schemaId, Object... args) throws IOException {
        TarantoolClientImpl.ByteArrayOutputStream bos = new TarantoolClientImpl.ByteArrayOutputStream(initialRequestSize);
        bos.write(new byte[5]);
        DataOutputStream ds = new DataOutputStream(bos);
        Map<Key, Object> header = new EnumMap<Key, Object>(Key.class);
        Map<Key, Object> body = new EnumMap<Key, Object>(Key.class);
        header.put(Key.CODE, code);
        header.put(Key.SYNC, syncId);
        if (schemaId != null) {
            header.put(Key.SCHEMA_ID, schemaId);
        }
        if (args != null) {
            for (int i = 0, e = args.length; i < e; i += 2) {
                Object value = args[i + 1];
                body.put((Key) args[i], value);
            }
        }
        msgPackLite.pack(header, ds);
        msgPackLite.pack(body, ds);
        ds.flush();
        ByteBuffer buffer = bos.toByteBuffer();
        buffer.put(0, (byte) 0xce);
        buffer.putInt(1, bos.size() - 5);
        return buffer;
    }

    protected void readPacket(DataInputStream is) throws IOException {
        int size = ((Number) msgPackLite.unpack(is)).intValue();
        long mark = cis.getBytesRead();
        headers = (Map<Integer, Object>) msgPackLite.unpack(is);
        if (cis.getBytesRead() - mark < size) {
            body = (Map<Integer, Object>) msgPackLite.unpack(is);
        }
        is.skipBytes((int) (cis.getBytesRead() - mark - size));
    }

    protected static class SQLMetaData {
        protected String name;

        public SQLMetaData(String name) {
            this.name = name;
        }

        public String getName() {
            return name;
        }

        @Override
        public String toString() {
            return "SQLMetaData{" +
                    "name='" + name + '\'' +
                    '}';
        }
    }

    protected List<SQLMetaData> getSQLMetadata() {
        List<Map<Integer, Object>> meta = (List<Map<Integer, Object>>) body.get(Key.SQL_METADATA.getId());
        List<SQLMetaData> values = new ArrayList<SQLMetaData>(meta.size());
        for(Map<Integer,Object> c:meta ) {
           values.add(new SQLMetaData((String) c.get(Key.SQL_FIELD_NAME.getId())));
        }
        return values;
    }

    protected List<List<Object>> getSQLData() {
        return (List<List<Object>>) body.get(Key.DATA.getId());
    }

    protected List<Map<String, Object>> readSqlResult(List<List<?>> data) {
        List<Map<String, Object>> values = new ArrayList<Map<String, Object>>(data.size());
        List<SQLMetaData> metaData = getSQLMetadata();
        LinkedHashMap<String, Object> value = new LinkedHashMap<String, Object>();
        for (List row : data) {
            for (int i = 0; i < row.size(); i++) {
                value.put(metaData.get(i).getName(), row.get(i));
            }
            values.add(value);
        }
        return values;
    }


    protected Long getSqlRowCount() {
        Map<Key, Object> info = (Map<Key, Object>) body.get(Key.SQL_INFO.getId());
        Number rowCount;
        if (info != null && (rowCount = ((Number) info.get(Key.SQL_ROW_COUNT.getId()))) != null) {
            return rowCount.longValue();
        }
        return null;
    }


    protected TarantoolException serverError(long code, Object error) {
        return new TarantoolException(code, error instanceof String ? (String) error : new String((byte[]) error));
    }

    protected class ByteArrayOutputStream extends java.io.ByteArrayOutputStream {
        public ByteArrayOutputStream(int size) {
            super(size);
        }

        ByteBuffer toByteBuffer() {
            return ByteBuffer.wrap(buf, 0, count);
        }
    }

    protected void closeChannel(SocketChannel channel) {
        if (channel != null) {
            try {
                channel.close();
            } catch (IOException ignored) {

            }
        }
    }

    protected void validateArgs(Object[] args) {
        if (args != null) {
            for (int i = 0; i < args.length; i += 2) {
                if (args[i + 1] == null) {
                    throw new NullPointerException(((Key) args[i]).name() + " should not be null");
                }
            }
        }
    }

    public void setInitialRequestSize(int initialRequestSize) {
        this.initialRequestSize = initialRequestSize;
    }
}
