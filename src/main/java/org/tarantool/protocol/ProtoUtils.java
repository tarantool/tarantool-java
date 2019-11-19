package org.tarantool.protocol;

import org.tarantool.Base64;
import org.tarantool.Code;
import org.tarantool.CommunicationException;
import org.tarantool.CountInputStreamImpl;
import org.tarantool.Key;
import org.tarantool.MsgPackLite;
import org.tarantool.TarantoolException;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.net.SocketAddress;
import java.net.SocketException;
import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.nio.channels.NonReadableChannelException;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.SocketChannel;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;

public abstract class ProtoUtils {

    public static final int LENGTH_OF_SIZE_MESSAGE = 5;

    private static final int DEFAULT_INITIAL_REQUEST_SIZE = 4096;
    private static final String WELCOME = "Tarantool ";

    /**
     * Reads tarantool binary protocol's packet from {@code inputStream}.
     *
     * @param inputStream ready to use input stream
     * @param msgPackLite MessagePack decoder instance
     *
     * @return Nonnull instance of packet
     *
     * @throws IOException in case of any io-error
     */
    public static TarantoolPacket readPacket(InputStream inputStream, MsgPackLite msgPackLite) throws IOException {
        CountInputStreamImpl msgStream = new CountInputStreamImpl(inputStream);

        int size = ((Number) msgPackLite.unpack(msgStream)).intValue();
        long mark = msgStream.getBytesRead();

        Map<Integer, Object> headers = (Map<Integer, Object>) msgPackLite.unpack(msgStream);

        Map<Integer, Object> body = null;
        if (msgStream.getBytesRead() - mark < size) {
            body = (Map<Integer, Object>) msgPackLite.unpack(msgStream);
        }

        return new TarantoolPacket(headers, body);
    }

    /**
     * Reads a tarantool's binary protocol packet from the reader.
     *
     * @param bufferReader readable channel that have to be in blocking mode
     *                     or instance of {@link ReadableViaSelectorChannel}
     * @param msgPackLite MessagePack decoder instance
     *
     * @return tarantool binary protocol message wrapped by instance of {@link TarantoolPacket}
     *
     * @throws IOException                 if any IO-error occurred during read from the channel
     * @throws CommunicationException      input stream bytes constitute msg pack message in wrong format
     * @throws NonReadableChannelException If this channel was not opened for reading
     */
    public static TarantoolPacket readPacket(ReadableByteChannel bufferReader, MsgPackLite msgPackLite)
        throws CommunicationException, IOException {

        ByteBuffer buffer = ByteBuffer.allocate(LENGTH_OF_SIZE_MESSAGE);
        bufferReader.read(buffer);

        buffer.flip();
        int size = ((Number) msgPackLite.unpack(new ByteBufferBackedInputStream(buffer))).intValue();

        buffer = ByteBuffer.allocate(size);
        bufferReader.read(buffer);

        buffer.flip();
        ByteBufferBackedInputStream msgBytesStream = new ByteBufferBackedInputStream(buffer);
        Object unpackedHeaders = msgPackLite.unpack(msgBytesStream);
        if (!(unpackedHeaders instanceof Map)) {
            //noinspection ConstantConditions
            throw new CommunicationException(
                "Error while unpacking headers of tarantool response: " +
                    "expected type Map but was " +
                    unpackedHeaders != null ? unpackedHeaders.getClass().toString() : "null"
            );
        }
        //noinspection unchecked (checked above)
        Map<Integer, Object> headers = (Map<Integer, Object>) unpackedHeaders;

        Map<Integer, Object> body = null;
        if (msgBytesStream.hasAvailable()) {
            Object unpackedBody = msgPackLite.unpack(msgBytesStream);
            if (!(unpackedBody instanceof Map)) {
                //noinspection ConstantConditions
                throw new CommunicationException(
                    "Error while unpacking body of tarantool response: " +
                        "expected type Map but was " +
                        unpackedBody != null ? unpackedBody.getClass().toString() : "null"
                );
            }
            //noinspection unchecked (checked above)
            body = (Map<Integer, Object>) unpackedBody;
        }

        return new TarantoolPacket(headers, body);
    }

    /**
     * Connects to a tarantool node described by {@code socket}. Performs an authentication if required
     *
     * @param socket   a socket channel to a tarantool node
     * @param username auth username
     * @param password auth password
     * @param msgPackLite MessagePack encoder / decoder instance
     *
     * @return object with information about a connection/
     *
     * @throws IOException            in case of any IO fails
     * @throws CommunicationException when welcome string is invalid
     * @throws TarantoolException     in case of failed authentication
     */
    public static TarantoolGreeting connect(Socket socket,
                                            String username,
                                            String password,
                                            MsgPackLite msgPackLite) throws IOException {
        byte[] inputBytes = new byte[64];

        InputStream inputStream = socket.getInputStream();
        inputStream.read(inputBytes);

        String firstLine = new String(inputBytes);
        assertCorrectWelcome(firstLine, socket.getRemoteSocketAddress());
        String serverVersion = firstLine.substring(WELCOME.length());

        inputStream.read(inputBytes);
        String salt = new String(inputBytes);
        if (username != null && password != null) {
            ByteBuffer authPacket = createAuthPacket(username, password, salt, msgPackLite);

            OutputStream os = socket.getOutputStream();
            os.write(authPacket.array(), 0, authPacket.remaining());
            os.flush();

            TarantoolPacket responsePacket = readPacket(socket.getInputStream(), msgPackLite);
            assertNoErrCode(responsePacket);
        }

        return new TarantoolGreeting(serverVersion);
    }

    /**
     * Connects to a tarantool node described by {@code socketChannel}. Performs an authentication if required.
     *
     * @param channel  a socket channel to tarantool node. The channel have to be in blocking mode
     * @param username auth username
     * @param password auth password
     * @param msgPackLite MessagePack encoder / decoder instance
     *
     * @return object with information about a connection/
     *
     * @throws IOException            in case of any IO fails
     * @throws CommunicationException when welcome string is invalid
     * @throws TarantoolException     in case of failed authentication
     */
    public static TarantoolGreeting connect(SocketChannel channel,
                                            String username,
                                            String password,
                                            MsgPackLite msgPackLite) throws IOException {
        ByteBuffer welcomeBytes = ByteBuffer.wrap(new byte[64]);
        channel.read(welcomeBytes);

        String firstLine = new String(welcomeBytes.array());
        assertCorrectWelcome(firstLine, channel.getRemoteAddress());
        final String serverVersion = firstLine.substring(WELCOME.length());

        ((Buffer)welcomeBytes).clear();
        channel.read(welcomeBytes);
        String salt = new String(welcomeBytes.array());

        if (username != null && password != null) {
            ByteBuffer authPacket = createAuthPacket(username, password, salt, msgPackLite);
            writeFully(channel, authPacket);

            TarantoolPacket authResponse = readPacket(channel, msgPackLite);
            assertNoErrCode(authResponse);
        }

        return new TarantoolGreeting(serverVersion);
    }

    private static void assertCorrectWelcome(String firstLine, SocketAddress remoteAddress) {
        if (!firstLine.startsWith(WELCOME)) {
            String errMsg = "Failed to connect to node " + remoteAddress.toString() +
                ": Welcome message should starts with tarantool but starts with '" +
                firstLine +
                "'";
            throw new CommunicationException(errMsg, new IllegalStateException("Invalid welcome packet"));
        }
    }

    private static void assertNoErrCode(TarantoolPacket authResponse) {
        Long code = (Long) authResponse.getHeaders().get(Key.CODE.getId());
        if (code != 0) {
            Object error = authResponse.getBody().get(Key.ERROR.getId());
            String errorMsg = error instanceof String ? (String) error : new String((byte[]) error);
            throw new TarantoolException(code, errorMsg);
        }
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

    public static ByteBuffer createAuthPacket(String username,
                                              final String password,
                                              String salt,
                                              MsgPackLite msgPackLite) throws IOException {
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

        return createPacket(
            DEFAULT_INITIAL_REQUEST_SIZE, msgPackLite,
            Code.AUTH, 0L, null, Key.USER_NAME, username, Key.TUPLE, auth
        );
    }

    public static ByteBuffer createPacket(MsgPackLite msgPackLite,
                                          Code code,
                                          Long syncId,
                                          Long schemaId,
                                          Object... args) throws IOException {
        return createPacket(DEFAULT_INITIAL_REQUEST_SIZE, msgPackLite, code, syncId, schemaId, args);
    }

    public static ByteBuffer createPacket(int initialRequestSize,
                                          MsgPackLite msgPackLite,
                                          Code code,
                                          Long syncId,
                                          Long schemaId,
                                          Object... args) throws IOException {
        ByteArrayOutputStream bos = new ByteArrayOutputStream(initialRequestSize);
        bos.write(new byte[5]);
        final DataOutputStream ds = new DataOutputStream(bos);
        Map<Key, Object> header = new EnumMap<>(Key.class);
        Map<Key, Object> body = new EnumMap<>(Key.class);
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

    private static class ByteArrayOutputStream extends java.io.ByteArrayOutputStream {
        public ByteArrayOutputStream(int size) {
            super(size);
        }

        ByteBuffer toByteBuffer() {
            return ByteBuffer.wrap(buf, 0, count);
        }
    }

}
