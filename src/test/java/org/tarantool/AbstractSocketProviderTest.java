package org.tarantool;

import static org.mockito.Mockito.anyObject;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketAddress;
import java.nio.channels.SocketChannel;
import java.util.Collection;
import java.util.stream.Collectors;

public class AbstractSocketProviderTest {

    protected String extractRawHostAndPortString(SocketAddress socketAddress) {
        InetSocketAddress inetSocketAddress = (InetSocketAddress) socketAddress;
        return inetSocketAddress.getAddress().getHostName() + ":" + inetSocketAddress.getPort();
    }

    protected Iterable<String> asRawHostAndPort(Collection<SocketAddress> addresses) {
        return addresses.stream()
                .map(this::extractRawHostAndPortString)
                .collect(Collectors.toList());
    }

    protected <T extends BaseSocketChannelProvider> T wrapWithMockChannelProvider(T source) throws IOException {
        T wrapper = spy(source);
        doReturn(makeSocketChannel()).when(wrapper).openChannel(anyObject());
        return wrapper;
    }

    protected <T extends BaseSocketChannelProvider> T wrapWithMockErroredChannelProvider(T source) throws IOException {
        T wrapper = spy(source);
        doThrow(IOException.class).when(wrapper).openChannel(anyObject());
        return wrapper;
    }

    private SocketChannel makeSocketChannel() {
        SocketChannel socketChannel = mock(SocketChannel.class);
        when(socketChannel.socket()).thenReturn(mock(Socket.class));

        return socketChannel;
    }

}
