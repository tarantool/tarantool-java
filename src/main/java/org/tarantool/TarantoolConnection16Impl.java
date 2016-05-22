package org.tarantool;

import java.io.IOException;
import java.lang.reflect.Field;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.tarantool.schema.FieldsMapping;
import org.tarantool.schema.IndexId;
import org.tarantool.schema.SchemaId;
import org.tarantool.schema.Space;
import org.tarantool.schema.SpaceId;


public class TarantoolConnection16Impl extends TarantoolConnection16Base<Integer,Object,Object,List> implements TarantoolConnection16 {

    public TarantoolConnection16Impl(SocketChannel channel) {
        super(channel);
    }

    public TarantoolConnection16Impl(String host, int port) throws IOException {
        this(SocketChannel.open(new InetSocketAddress(host, port)));
    }

    public synchronized List exec(Code code, Object... args) {
        write(code, args);
        return (List) readData();
    }
}
