package org.tarantool.server;

import org.tarantool.Base64;
import org.tarantool.ByteBufferInputStream;
import org.tarantool.Code;
import org.tarantool.CommunicationException;
import org.tarantool.CountInputStream;
import org.tarantool.CountInputStreamImpl;
import org.tarantool.Key;
import org.tarantool.MsgPackLite;
import org.tarantool.TarantoolException;

import java.io.*;
import java.net.Socket;
import java.net.SocketException;
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
    public static final String WELCOME = "Tarantool ";

    public static TarantoolBinaryPackage readPacket(InputStream inputStream) throws IOException {
        int size = inputStream.read();

        CountInputStreamImpl msgStream = new CountInputStreamImpl(inputStream);
        Map<Integer, Object> headers = (Map<Integer, Object>) getMsgPackLite().unpack(msgStream);


        Map<Integer, Object> body = null;
        if (msgStream.getBytesRead() < size) {
            body = (Map<Integer, Object>) getMsgPackLite().unpack(msgStream);
        }

        return new TarantoolBinaryPackage(headers, body);
    }

    /**
     * Connects to a tarantool node described by {@code socket}. Performs an authentication if required
     *
     * @param socket   a socket channel to tarantool node
     * @param username auth username
     * @param password auth password
     * @return object with information about a connection/
     * @throws IOException            in case of any IO fails
     * @throws CommunicationException when welcome string is invalid
     * @throws TarantoolException     in case of failed authentication
     */
    public static TarantoolInstanceConnectionMeta connect(Socket socket, String username, String password) throws IOException {
        byte[] inputBytes = new byte[64];

        InputStream inputStream = socket.getInputStream();
        inputStream.read(inputBytes);

        String firstLine = new String(inputBytes);
        if (!firstLine.startsWith(WELCOME)) {
            String errMsg = "Failed to connect to node " + socket.getRemoteSocketAddress().toString() + ":" +
                    " Welcome message should starts with tarantool but starts with '" + firstLine + "'";
            throw new CommunicationException(errMsg, new IllegalStateException("Invalid welcome packet"));
        }

        String serverVersion = firstLine.substring(WELCOME.length());

        inputStream.read(inputBytes);
        String salt = new String(inputBytes);
        if (username != null && password != null) {
            ByteBuffer authPacket = createAuthPacket(username, password, salt);

            OutputStream os = socket.getOutputStream();
            os.write(authPacket.array(), 0, authPacket.remaining());
            os.flush();

            TarantoolBinaryPackage responsePackage = readPacket(socket.getInputStream());
            Long code = (Long) responsePackage.getHeaders().get(Key.CODE.getId());
            if (code != 0) {
                Object error = responsePackage.getBody().get(Key.ERROR.getId());
                throw new TarantoolException(code, error instanceof String ? (String) error : new String((byte[]) error));
            }
        }

        return new TarantoolInstanceConnectionMeta(salt, serverVersion);
    }

    /**
     * Connects to a tarantool node described by {@code socketChannel}. Performs an authentication if required
     *
     * @param channel  a socket channel to tarantool node
     * @param username auth username
     * @param password auth password
     * @return object with information about a connection/
     * @throws IOException            in case of any IO fails
     * @throws CommunicationException when welcome string is invalid
     * @throws TarantoolException     in case of failed authentication
     */
    public static TarantoolInstanceConnectionMeta connect(SocketChannel channel, String username, String password) throws IOException {
        ByteBuffer welcomeBytes = ByteBuffer.wrap(new byte[64]);
        channel.read(welcomeBytes);

        String firstLine = new String(welcomeBytes.array());
        if (!firstLine.startsWith(WELCOME)) {
            String errMsg = "Failed to connect to node " + channel.getRemoteAddress().toString() + ":" +
                    " Welcome message should starts with tarantool but starts with '" + firstLine + "'";
            throw new CommunicationException(errMsg, new IllegalStateException("Invalid welcome packet"));
        }
        String serverVersion = firstLine.substring(WELCOME.length());

        welcomeBytes.clear();
        channel.read(welcomeBytes);
        String salt = new String(welcomeBytes.array());
        if (username != null && password != null) {
            ByteBuffer authPacket = createAuthPacket(username, password, salt);
            writeFully(channel, authPacket);
            TarantoolBinaryPackage authResponse = readPacket(channel);
            Long code = (Long) authResponse.getHeaders().get(Key.CODE.getId());
            if (code != 0) {
                Object error = authResponse.getBody().get(Key.ERROR.getId());
                throw new TarantoolException(code, error instanceof String ? (String) error : new String((byte[]) error));
            }
        }

        return new TarantoolInstanceConnectionMeta(salt, serverVersion);
    }

    public static void writeFully(OutputStream stream, ByteBuffer buffer) throws IOException {
        stream.write(buffer.array());
        stream.flush();
    }

    public static void writeFully(SocketChannel channel, ByteBuffer buffer) throws IOException {
        long code = 0;
        while (buffer.remaining() > 0 && (code = channel.write(buffer)) > -1) {
        }
        if (code < 0) {
            throw new SocketException("write failed code: " + code);
        }
    }

    public static final int LENGTH_OF_SIZE_MESSAGE = 5;
    public static TarantoolBinaryPackage readPacket(SocketChannel channel) throws IOException {
        channel.configureBlocking(false);

        //todo get rid of this because SelectorProvider.provider().openSelector() creates two pipes and socket in the /proc/fd
        SelectorChannelReadHelper bufferReader = new SelectorChannelReadHelper(channel);

        ByteBuffer buffer = ByteBuffer.allocate(LENGTH_OF_SIZE_MESSAGE);
        bufferReader.readFully(buffer);

        buffer.flip();
        int size = ((Number) getMsgPackLite().unpack(new ByteBufferBackedInputStream(buffer))).intValue();

        buffer = ByteBuffer.allocate(size);
        bufferReader.readFully(buffer);

        buffer.flip();
        ByteBufferBackedInputStream msgBytesStream = new ByteBufferBackedInputStream(buffer);
        Object unpackedHeaders = getMsgPackLite().unpack(msgBytesStream);
        if (!(unpackedHeaders instanceof Map)) {
            //noinspection ConstantConditions
            throw new CommunicationException("Error while unpacking headers of tarantool response: " +
                    "expected type Map but was " + unpackedHeaders != null ? unpackedHeaders.getClass().toString() : "null");
        }
        //noinspection unchecked (checked above)
        Map<Integer, Object> headers = (Map<Integer, Object>) unpackedHeaders;

        Map<Integer, Object> body = null;
        if (msgBytesStream.hasAvailable()) {
            Object unpackedBody = getMsgPackLite().unpack(msgBytesStream);
            if (!(unpackedBody instanceof Map)) {
                //noinspection ConstantConditions
                throw new CommunicationException("Error while unpacking body of tarantool response: " +
                        "expected type Map but was " + unpackedBody != null ? unpackedBody.getClass().toString() : "null");
            }
            //noinspection unchecked (checked above)
            body = (Map<Integer, Object>) unpackedBody;
        }

        return new TarantoolBinaryPackage(headers, body);
    }

    @Deprecated
    public static TarantoolBinaryPackage readPacketOld(SocketChannel channel) throws IOException {
        CountInputStream inputStream = new ByteBufferInputStream(channel);
        return readPacket(inputStream);
    }

    @Deprecated
    private static TarantoolBinaryPackage readPacket(CountInputStream inputStream) throws IOException {
        int size = ((Number) getMsgPackLite().unpack(inputStream)).intValue();

        long mark = inputStream.getBytesRead();

        Object unpackedHeaders = getMsgPackLite().unpack(inputStream);
        if (!(unpackedHeaders instanceof Map)) {
            //noinspection ConstantConditions
            throw new CommunicationException("Error while unpacking headers of tarantool response: " +
                    "expected type Map but was " + unpackedHeaders != null ? unpackedHeaders.getClass().toString() : "null");
        }
        //noinspection unchecked (checked above)
        Map<Integer, Object> headers = (Map<Integer, Object>) unpackedHeaders;

        Map<Integer, Object> body = null;
        if (inputStream.getBytesRead() - mark < size) {
            Object unpackedBody = getMsgPackLite().unpack(inputStream);
            if (!(unpackedBody instanceof Map)) {
                //noinspection ConstantConditions
                throw new CommunicationException("Error while unpacking body of tarantool response: " +
                        "expected type Map but was " + unpackedBody != null ? unpackedBody.getClass().toString() : "null");
            }
            //noinspection unchecked (checked above)
            body = (Map<Integer, Object>) unpackedBody;
        }

        return new TarantoolBinaryPackage(headers, body);
    }


    public static ByteBuffer createAuthPacket(String username, final String password, String salt) throws IOException {
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

    public static ByteBuffer createPacket(Code code, Long syncId, Long schemaId, Object... args) throws IOException {
        return createPacket(DEFAULT_INITIAL_REQUEST_SIZE, code, syncId, schemaId, args);
    }

    public static ByteBuffer createPacket(int initialRequestSize, Code code, Long syncId, Long schemaId, Object... args) throws IOException {
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

    private static MsgPackLite getMsgPackLite() {
        return MsgPackLite.INSTANCE;
    }
}
