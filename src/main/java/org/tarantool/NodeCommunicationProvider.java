package org.tarantool;

import org.tarantool.server.*;

import java.io.*;
import java.nio.*;

public interface NodeCommunicationProvider {

    TarantoolInstanceConnection connect() throws IOException;

    String getDescription();
}
