package org.tarantool;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertIterableEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Matchers.anyObject;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;
import static org.tarantool.TestUtils.asRawHostAndPort;
import static org.tarantool.TestUtils.extractRawHostAndPortString;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.Socket;
import java.nio.channels.SocketChannel;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

@DisplayName("A RR socket provider")
public class RoundRobinSocketProviderImplTest {

    @Test
    @DisplayName("initialized with a right addresses count")
    public void testAddressesCount() {
        RoundRobinSocketProviderImpl socketProvider
                = new RoundRobinSocketProviderImpl("localhost:3301", "127.0.0.1:3302", "10.0.0.10:3303");
        assertEquals(3, socketProvider.getAddressCount());

        socketProvider.refreshAddresses(Collections.singletonList("10.0.0.1"));
        assertEquals(1, socketProvider.getAddressCount());
    }

    @Test
    @DisplayName("initialized with a right addresses values")
    public void testAddresses() {
        String[] addresses = {"localhost:3301", "127.0.0.2:3302", "10.0.0.10:3303"};
        RoundRobinSocketProviderImpl socketProvider
                = new RoundRobinSocketProviderImpl(addresses);
        assertIterableEquals(Arrays.asList(addresses), asRawHostAndPort(socketProvider.getAddresses()));

        List<String> strings = Collections.singletonList("10.0.0.1:3310");
        socketProvider.refreshAddresses(strings);
        assertIterableEquals(strings, asRawHostAndPort(socketProvider.getAddresses()));
    }

    @Test
    @DisplayName("initialized failed when an empty addresses list is provided")
    public void testEmptyAddresses() {
        assertThrows(IllegalArgumentException.class, RoundRobinSocketProviderImpl::new);
    }

    @Test
    @DisplayName("changed addresses list with a failure when a new list is empty")
    public void testResultWithEmptyAddresses() throws IOException {
        RoundRobinSocketProviderImpl socketProvider
                = wrapWithMockChannelProvider(new RoundRobinSocketProviderImpl("localhost:3301"));

        assertThrows(IllegalArgumentException.class, () -> socketProvider.refreshAddresses(Collections.emptyList()));
    }

    @Test
    @DisplayName("changed addresses list with a failure when a new list is null")
    public void testResultWithWrongAddress() throws IOException {
        RoundRobinSocketProviderImpl socketProvider
                = wrapWithMockChannelProvider(new RoundRobinSocketProviderImpl("localhost:3301"));

        assertThrows(IllegalArgumentException.class, () -> socketProvider.refreshAddresses(null));
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
        int negativeValue = -200;
        assertThrows(IllegalArgumentException.class, () -> socketProvider.setConnectionTimeout(negativeValue));
    }

    @Test
    @DisplayName("produced socket channels using a ring pool")
    public void testAddressRingPool() throws IOException {
        String[] addresses = {"localhost:3301", "10.0.0.1:3302", "10.0.0.2:3309"};
        RoundRobinSocketProviderImpl socketProvider
                = wrapWithMockChannelProvider(new RoundRobinSocketProviderImpl(addresses));

        for (int i = 0; i < 27; i++) {
            socketProvider.get(0, null);
            assertEquals(addresses[i % 3], extractRawHostAndPortString(socketProvider.getLastObtainedAddress()));
        }
    }

    @Test
    @DisplayName("produced socket channels for the same instance")
    public void testOneAddressPool() throws IOException {
        String expectedAddress = "10.0.0.1:3301";
        String[] addresses = {expectedAddress};
        RoundRobinSocketProviderImpl socketProvider
                = wrapWithMockChannelProvider(new RoundRobinSocketProviderImpl(addresses));

        for (int i = 0; i < 5; i++) {
            socketProvider.get(0, null);
            assertEquals(expectedAddress, extractRawHostAndPortString(socketProvider.getLastObtainedAddress()));
        }
    }

    @Test
    @DisplayName("produced socket channel with an exception when an attempt number is over")
    public void testTooManyAttempts() throws IOException {
        String expectedAddress = "10.0.0.1:3301";
        String[] addresses = {expectedAddress};
        RoundRobinSocketProviderImpl socketProvider
                = wrapWithMockChannelProvider(new RoundRobinSocketProviderImpl(addresses));

        int retriesLimit = addresses.length + 1;
        socketProvider.setRetriesLimit(retriesLimit);

        for (int i = 0; i < retriesLimit; i++) {
            socketProvider.get(i, null);
            assertEquals(expectedAddress, extractRawHostAndPortString(socketProvider.getLastObtainedAddress()));
        }

        assertThrows(CommunicationException.class, () -> socketProvider.get(retriesLimit, null));
    }

    @Test
    @DisplayName("produced a socket channel with a failure when an unreachable address is provided")
    public void testWrongAddress() throws IOException {
        RoundRobinSocketProviderImpl socketProvider
                = wrapWithMockErroredChannelProvider(new RoundRobinSocketProviderImpl("unreachable-host:3301"));
        assertThrows(SocketProviderTransientException.class, () -> socketProvider.get(0, null));
    }

    @Test
    @DisplayName("produced a socket channel with a failure with an unreachable address after refresh")
    public void testWrongRefreshAddress() throws IOException {
        RoundRobinSocketProviderImpl socketProvider
                = wrapWithMockErroredChannelProvider(new RoundRobinSocketProviderImpl("unreachable-host:3301"));
        assertThrows(SocketProviderTransientException.class, () -> socketProvider.get(0, null));
    }

    private <T extends BaseSocketChannelProvider> T wrapWithMockChannelProvider(T source) throws IOException {
        T wrapper = spy(source);
        doReturn(makeSocketChannel()).when(wrapper).openChannel(anyObject());
        return wrapper;
    }

    private <T extends BaseSocketChannelProvider> T wrapWithMockErroredChannelProvider(T source) throws IOException {
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
