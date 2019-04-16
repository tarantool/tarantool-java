package org.tarantool;

import org.tarantool.protocol.ProtoUtils;
import org.tarantool.protocol.TarantoolPacket;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.net.SocketException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;

public class TarantoolConnection extends TarantoolBase<List<?>>
        implements TarantoolSQLOps<Object,Long,List<Map<String,Object>>> {

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
        TarantoolPacket responsePacket = writeAndRead(code, args);
        return (List) responsePacket.getBody().get(Key.DATA.getId());
    }

    protected TarantoolPacket writeAndRead(Code code, Object... args) {
        try {
            ByteBuffer packet = ProtoUtils.createPacket(initialRequestSize, msgPackLite,
                code, syncId.incrementAndGet(), null, args);

            out.write(packet.array(), 0, packet.remaining());
            out.flush();

            TarantoolPacket responsePacket = ProtoUtils.readPacket(in);

            Long c = responsePacket.getCode();
            if (c != 0) {
                throw serverError(c, responsePacket.getBody().get(Key.ERROR.getId()));
            }

            return responsePacket;
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

    /**
     * Closes current connection.
     */
    public void close() {
        try {
            socket.close();
        } catch (IOException ignored) {
            // No-op
        }
    }

    @Override
    public Long update(String sql, Object... bind) {
        TarantoolPacket pack = sql(sql, bind);
        return SqlProtoUtils.getSqlRowCount(pack);
    }

    @Override
    public List<Map<String, Object>> query(String sql, Object... bind) {
        TarantoolPacket pack = sql(sql, bind);
        return SqlProtoUtils.readSqlResult(pack);
    }

    protected TarantoolPacket sql(String sql, Object[] bind) {
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
