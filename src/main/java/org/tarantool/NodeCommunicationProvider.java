package org.tarantool;

import org.tarantool.server.*;

import java.io.*;
import java.nio.*;

public interface NodeCommunicationProvider {

    void connect() throws IOException;

    TarantoolBinaryPackage readPackage() throws IOException;

    void writeBuffer(ByteBuffer byteBuffer) throws IOException;

    String getDescription();
}
