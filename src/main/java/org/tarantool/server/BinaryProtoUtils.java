package org.tarantool.server;

import org.tarantool.Base64;
import org.tarantool.Code;
import org.tarantool.CountInputStreamImpl;
import org.tarantool.Key;
import org.tarantool.MsgPackLite;
import org.tarantool.server.TarantoolServer.CountingInputStream;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;

public abstract class BinaryProtoUtils {

    private final static int DEFAULT_INITIAL_REQUEST_SIZE = 4096;


    private TarantoolBinaryPackage readPacket(SocketChannel channel) throws IOException {

        int size = channel.socket().getInputStream().read();
        ByteBuffer msgBuffer = ByteBuffer.allocate(size - 1);
        channel.read(msgBuffer);

        CountInputStreamImpl msgStream = new CountInputStreamImpl(new ByteArrayInputStream(msgBuffer.array()));
        Map<Integer, Object> headers = (Map<Integer, Object>) getMsgPackLite().unpack(msgStream);


        Map<Integer, Object> body = null;
        if (msgStream.getBytesRead() < size) {
            body = (Map<Integer, Object>) getMsgPackLite().unpack(msgStream);
        }

        return new TarantoolBinaryPackage(headers, body);
    }


    protected static ByteBuffer createAuthPacket(String username, final String password, String salt) throws IOException {
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

        // this was the default implementation
//        return createPacket(initialRequestSize, Code.AUTH, 0L, null, Key.USER_NAME, username, Key.TUPLE, auth);
        return createPacket(DEFAULT_INITIAL_REQUEST_SIZE, Code.AUTH, 0L, null, Key.USER_NAME, username, Key.TUPLE, auth);
    }

    protected static ByteBuffer createPacket(Code code, Long syncId, Long schemaId, Object... args) throws IOException {
        return createPacket(DEFAULT_INITIAL_REQUEST_SIZE, code, syncId, schemaId, args);
    }

    protected static ByteBuffer createPacket(int initialRequestSize, Code code, Long syncId, Long schemaId, Object... args) throws IOException {
//        TarantoolClientImpl.ByteArrayOutputStream bos = new TarantoolClientImpl.ByteArrayOutputStream(initialRequestSize);
        ByteArrayOutputStream bos = new ByteArrayOutputStream(initialRequestSize);
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
        getMsgPackLite().pack(header, ds);
        getMsgPackLite().pack(body, ds);
        ds.flush();
        ByteBuffer buffer = ByteBuffer.wrap(bos.toByteArray(), 0, bos.size());
        buffer.put(0, (byte) 0xce);
        buffer.putInt(1, bos.size() - 5);
        return buffer;
    }

    public MsgPackLite getMsgPackLite() {
        return MsgPackLite.INSTANCE;
    }
}
