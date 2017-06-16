package org.tarantool;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.Collections;
import java.util.List;

import org.junit.Assert;
import org.junit.Test;

public class TarantoolConnectionTest {

	@Test
	public void testGuestConnect() throws IOException {
		Socket socket = new Socket();
		socket.connect(new InetSocketAddress("127.0.0.1", 3301));
		TarantoolConnection con = new TarantoolConnection(null, null, socket);
		List<?> result = con.select(281, 0, Collections.emptyList(), 0, 1024, 0);
		Assert.assertFalse(result.isEmpty());
		con.close();
	}

}
