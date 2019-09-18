package org.tarantool;

import org.tarantool.protocol.ProtoUtils;
import org.tarantool.protocol.TarantoolGreeting;

import java.io.IOException;
import java.net.Socket;
import java.nio.channels.SocketChannel;
import java.util.concurrent.atomic.AtomicLong;

public abstract class TarantoolBase<Result> extends AbstractTarantoolOps<Result> {
    protected String serverVersion;
    protected MsgPackLite msgPackLite = MsgPackLite.INSTANCE;
    protected AtomicLong syncId = new AtomicLong();
    protected int initialRequestSize = 4096;

    public TarantoolBase() {
    }

    public TarantoolBase(String username, String password, Socket socket) {
        super();
        try {
            TarantoolGreeting greeting = ProtoUtils.connect(socket, username, password, msgPackLite);
            this.serverVersion = greeting.getServerVersion();
        } catch (IOException e) {
            throw new CommunicationException("Couldn't connect to tarantool", e);
        }
    }

    protected TarantoolException serverError(long code, Object error) {
        return new TarantoolException(code, error instanceof String ? (String) error : new String((byte[]) error));
    }

    protected void closeChannel(SocketChannel channel) {
        if (channel != null) {
            try {
                channel.close();
            } catch (IOException ignored) {
                // no-op
            }
        }
    }

    public void setInitialRequestSize(int initialRequestSize) {
        this.initialRequestSize = initialRequestSize;
    }

    public String getServerVersion() {
        return serverVersion;
    }
}
