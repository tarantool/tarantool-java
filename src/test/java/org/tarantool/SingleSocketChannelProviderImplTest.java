package org.tarantool;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Matchers.anyObject;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;
import static org.tarantool.TestUtils.extractRawHostAndPortString;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.Socket;
import java.nio.channels.SocketChannel;

@DisplayName("A single socket provider")
class SingleSocketChannelProviderImplTest {

    @Test
    @DisplayName("initialized with a right address")
    public void testAddressesCount() {
        String expectedAddress = "localhost:3301";
        SingleSocketChannelProviderImpl socketProvider
                = new SingleSocketChannelProviderImpl(expectedAddress);
        assertEquals(expectedAddress, extractRawHostAndPortString(socketProvider.getAddress()));
    }

    @Test
    @DisplayName("poorly initialized with an empty address")
    public void testEmptyAddresses() {
        assertThrows(IllegalArgumentException.class, () -> new SingleSocketChannelProviderImpl(null));
    }

    @Test
    @DisplayName("initialized with a default timeout")
    public void testDefaultTimeout() {
        RoundRobinSocketProviderImpl socketProvider
                = new RoundRobinSocketProviderImpl("localhost");
        assertEquals(RoundRobinSocketProviderImpl.NO_TIMEOUT, socketProvider.getConnectionTimeout());
    }

    @Test
    @DisplayName("changed its timeout to new value")
    public void testChangingTimeout() {
        RoundRobinSocketProviderImpl socketProvider
                = new RoundRobinSocketProviderImpl("localhost");
        int expectedTimeout = 10_000;
        socketProvider.setConnectionTimeout(expectedTimeout);
        assertEquals(expectedTimeout, socketProvider.getConnectionTimeout());
    }

    @Test
    @DisplayName("changed to negative timeout with a failure")
    public void testWrongChangingTimeout() {
        RoundRobinSocketProviderImpl socketProvider
                = new RoundRobinSocketProviderImpl("localhost");
        int negativeValue = -100;
        assertThrows(IllegalArgumentException.class, () -> socketProvider.setConnectionTimeout(negativeValue));
    }

    @Test
    @DisplayName("produced sockets with same address")
    public void testMultipleChannelGetting() throws IOException {
        String expectedAddresss = "localhost:3301";
        SingleSocketChannelProviderImpl socketProvider
                = wrapWithMockChannelProvider(new SingleSocketChannelProviderImpl(expectedAddresss));

        for (int i = 0; i < 10; i++) {
            socketProvider.get(0, null);
            assertEquals(expectedAddresss, extractRawHostAndPortString(socketProvider.getAddress()));
        }
    }

    private <T extends BaseSocketChannelProvider> T wrapWithMockChannelProvider(T source) throws IOException {
        T wrapper = spy(source);
        doReturn(makeSocketChannel()).when(wrapper).openChannel(anyObject());
        return wrapper;
    }

    private SocketChannel makeSocketChannel() {
        SocketChannel socketChannel = mock(SocketChannel.class);
        when(socketChannel.socket()).thenReturn(mock(Socket.class));

        return socketChannel;
    }

}
