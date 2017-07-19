package org.tarantool.jdbc;

import java.net.Socket;
import java.net.URI;
import java.util.Properties;

public interface SQLSocketProvider {

    Socket getConnectedSocket(URI uri, Properties params);
}
