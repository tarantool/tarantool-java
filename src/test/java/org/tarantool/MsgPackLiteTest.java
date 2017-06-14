package org.tarantool;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.msgpack.core.MessageBufferPacker;
import org.msgpack.core.MessagePack;

public class MsgPackLiteTest {
	private MessageBufferPacker packer;

	@Before
	public void setup() {
		packer = MessagePack.newDefaultBufferPacker();
	}

	@After
	public void tearDown() throws IOException {
		packer.close();
	}

	@Test
	public void testNull() throws IOException {
		packer.packNil();
		byte[] actual = packActual(null);
		Assert.assertArrayEquals(packer.toByteArray(), actual);
	}

	@Test
	public void testInt() throws IOException {
		packer.packInt(Integer.MAX_VALUE);
		byte[] actual = packActual(Integer.MAX_VALUE);
		Assert.assertArrayEquals(packer.toByteArray(), actual);
	}

	@Test
	public void testLong() throws IOException {
		packer.packLong(Long.MAX_VALUE);
		byte[] actual = packActual(Long.MAX_VALUE);
		Assert.assertArrayEquals(packer.toByteArray(), actual);
	}

	@Test
	public void testString() throws IOException {
		packer.packString("FooBar");
		byte[] actual = packActual("FooBar");
		Assert.assertArrayEquals(packer.toByteArray(), actual);
	}

	@Test
	public void testArray() throws IOException {
		packer.packArrayHeader(2);
		packer.packString("Foo");
		packer.packString("Bar");

		byte[] actual = packActual(new String[] { "Foo", "Bar" });
		Assert.assertArrayEquals(packer.toByteArray(), actual);
	}

	@Test
	public void testList() throws IOException {
		packer.packArrayHeader(2);
		packer.packString("Foo");
		packer.packString("Bar");

		byte[] actual = packActual(Arrays.asList("Foo", "Bar"));
		Assert.assertArrayEquals(packer.toByteArray(), actual);
	}

	@Test
	public void testBigList() throws IOException {
		packer.packArrayHeader(100);
		for (int i = 0; i < 100; i++) {
			packer.packString("Foo");
		}

		byte[] actual = packActual(Collections.nCopies(100, "Foo"));
		Assert.assertArrayEquals(packer.toByteArray(), actual);
	}

	@Test
	public void testMap() throws IOException {
		packer.packMapHeader(2);
		packer.packString("Foo");
		packer.packString("Bar");
		packer.packString("Bar");
		packer.packString("Foo");

		Map<String, String> map = new LinkedHashMap<String, String>();
		map.put("Foo", "Bar");
		map.put("Bar", "Foo");
		byte[] actual = packActual(map);
		Assert.assertArrayEquals(packer.toByteArray(), actual);
	}

	private static byte[] packActual(Object obj) throws IOException {
		ByteArrayOutputStream out = new ByteArrayOutputStream();
		MsgPackLite.INSTANCE.pack(obj, out);
		return out.toByteArray();
	}
}
