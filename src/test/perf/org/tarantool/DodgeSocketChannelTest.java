package org.tarantool;

import org.junit.jupiter.api.*;

import java.io.*;
import java.nio.*;

public class DodgeSocketChannelTest {


    @Test
    void testReadWellcome() throws IOException {
        DodgeSocketChannel dodgeSocketChannel = new DodgeSocketChannel(10);

        ByteBuffer buffer = readWellcome(dodgeSocketChannel);

        Assertions.assertEquals(DodgeSocketChannel.FAKE_WELCOME_STRING, new String(buffer.array()));
    }

    private ByteBuffer readWellcome(DodgeSocketChannel dodgeSocketChannel) throws IOException {
        ByteBuffer buffer = ByteBuffer.wrap(new byte[DodgeSocketChannel.FAKE_WELCOME_STRING.length()]);
        dodgeSocketChannel.read(buffer);
        return buffer;
    }

    @Test
    void testCorrectDodge() throws IOException {
        DodgeSocketChannel dodgeSocketChannel = new DodgeSocketChannel(10);

        readWellcome(dodgeSocketChannel);

        dodgeSocketChannel.write(ByteBuffer.wrap("one".getBytes()));
        dodgeSocketChannel.write(ByteBuffer.wrap("two".getBytes()));

        ByteBuffer oneBuffer = ByteBuffer.allocate("one".getBytes().length);
        ByteBuffer twoBuffer = ByteBuffer.allocate("two".getBytes().length);
        dodgeSocketChannel.read(oneBuffer);
        dodgeSocketChannel.read(twoBuffer);

        Assertions.assertEquals("one", new String(oneBuffer.array()));
        Assertions.assertEquals("two", new String(twoBuffer.array()));
    }
}
