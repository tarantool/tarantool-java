package org.tarantool;

import java.net.SocketAddress;
import java.util.Collection;

public interface RefreshableSocketProvider {

    Collection<SocketAddress> getAddresses();

    void refreshAddresses(Collection<String> addresses);

}
