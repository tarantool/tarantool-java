package org.tarantool;

import org.tarantool.server.BinaryProtoUtils;
import org.tarantool.server.TarantoolBinaryPacket;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.net.SocketException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;

public class TarantoolConnection extends TarantoolBase<List<?>> implements TarantoolSQLOps<Object,Long,List<Map<String,Object>>> {
    protected InputStream in;
    protected OutputStream out;
    protected Socket socket;


    public TarantoolConnection(String username, String password, Socket socket) throws IOException {
        super(username, password, socket);
        this.socket = socket;
        this.out = socket.getOutputStream();
        this.in = socket.getInputStream();
    }

    @Override
    protected List<?> exec(Code code, Object... args) {
        TarantoolBinaryPacket responsePackage = writeAndRead(code, args);
        return (List) responsePackage.getBody().get(Key.DATA.getId());
    }

    protected TarantoolBinaryPacket writeAndRead(Code code, Object... args) {
        try {
            ByteBuffer packet = BinaryProtoUtils.createPacket(
                    initialRequestSize,
                    msgPackLite,
                    code,
                    syncId.incrementAndGet(),
                    null,
                    args
            );

            out.write(packet.array(), 0, packet.remaining());
            out.flush();

            TarantoolBinaryPacket responsePackage = BinaryProtoUtils.readPacket(in);

            Map<Integer, Object> headers = responsePackage.getHeaders();
            Map<Integer, Object> body = responsePackage.getBody();
            Long c = (Long) headers.get(Key.CODE.getId());

            if (c != 0) {
                throw serverError(c, body.get(Key.ERROR.getId()));
            }

            return responsePackage;
        } catch (IOException e) {
            close();
            throw new CommunicationException("Couldn't execute query", e);
        }
    }

    public void begin() {
        call("box.begin");
    }

    public void commit() {
        call("box.commit");
    }

    public void rollback() {
        call("box.rollback");
    }

    public void close() {
        try {
            socket.close();
        } catch (IOException ignored) {

        }
    }

    @Override
    public Long update(String sql, Object... bind) {
        TarantoolBinaryPacket pack = sql(sql, bind);
        return SqlProtoUtils.getSqlRowCount(pack);
    }

    @Override
    public List<Map<String, Object>> query(String sql, Object... bind) {
        TarantoolBinaryPacket pack = sql(sql, bind);
        return SqlProtoUtils.readSqlResult(pack);
    }

    protected TarantoolBinaryPacket sql(String sql, Object[] bind) {
        return writeAndRead(Code.EXECUTE, Key.SQL_TEXT, sql, Key.SQL_BIND, bind);
    }

    public boolean isClosed() {
        return socket.isClosed();
    }

    /**
     * Sets given timeout value on underlying socket.
     *
     * @param timeout Timeout in milliseconds.
     * @throws SocketException If failed.
     */
    public void setSocketTimeout(int timeout) throws SocketException {
        socket.setSoTimeout(timeout);
    }

    /**
     * Retrieves timeout value from underlying socket.
     *
     * @return Timeout in milliseconds.
     * @throws SocketException If failed.
     */
    public int getSocketTimeout() throws SocketException {
        return socket.getSoTimeout();
    }
}
