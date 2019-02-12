package org.tarantool;

import org.junit.jupiter.api.*;

import java.io.*;
import java.net.*;
import java.nio.*;
import java.nio.channels.*;
import java.nio.channels.spi.*;
import java.util.*;
import java.util.concurrent.*;

public class DodgeSocketChannelTest {

    @Test
    void testCorrectDodge() throws IOException {
        DodgeSocketChannel dodgeSocketChannel = new DodgeSocketChannel(10);


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
