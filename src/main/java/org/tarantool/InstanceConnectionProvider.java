package org.tarantool;

import org.tarantool.server.*;

import java.io.*;
import java.nio.*;

public interface InstanceConnectionProvider {

    TarantoolInstanceConnection connect() throws IOException;

    String getDescription();
}
