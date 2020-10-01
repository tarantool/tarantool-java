package org.tarantool;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.tarantool.conversion.NotConvertibleValueException;
import org.tarantool.dsl.Requests;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;

public class ClientResultSetIT {

    private static TarantoolTestHelper testHelper;

    private TarantoolClient client;

    @BeforeAll
    static void setupEnv() {
        testHelper = new TarantoolTestHelper("client-resultset-it");
        testHelper.createInstance();
        testHelper.startInstance();
    }

    @BeforeEach
    void setUp() {
        client = TestUtils.makeTestClient(TestUtils.makeDefaultClientConfig(), 2000);
    }

    @AfterEach
    void tearDown() {
        client.close();
    }

    @AfterAll
    static void tearDownEnv() {
        testHelper.stopInstance();
    }

    @Test
    void testGetSimpleRows() {
        testHelper.executeLua(
            "box.schema.space.create('basic_test', { format = " +
                "{{name = 'id', type = 'integer'}," +
                "{name = 'num', type = 'integer', is_nullable = true}," +
                "{name = 'val', type = 'string', is_nullable = true} }})",
            "box.space.basic_test:create_index('pk', { type = 'TREE', parts = {'id'} } )",
            "box.space.basic_test:insert{1, nil, 'string'}",
            "box.space.basic_test:insert{2, 50, nil}",
            "box.space.basic_test:insert{3, 123, 'some'}",
            "box.space.basic_test:insert{4, -89, '89'}",
            "box.space.basic_test:insert{5, 93127, 'too many'}",
            "box.space.basic_test:insert{6, nil, nil}"
        );

        TarantoolResultSet resultSet = client.executeRequest(Requests.selectRequest("basic_test", "pk"));
        resultSet.next();
        assertEquals(0, resultSet.getInt(2));
        assertEquals("string", resultSet.getString(3));

        // all ending nil values are trimmed
        resultSet.next();
        assertEquals(50, resultSet.getInt(2));
        assertEquals(2, resultSet.getRowSize());

        resultSet.next();
        assertEquals(123, resultSet.getInt(2));
        assertEquals("some", resultSet.getString(3));

        resultSet.next();
        assertEquals(-89, resultSet.getInt(2));
        assertEquals("89", resultSet.getString(3));

        resultSet.next();
        assertEquals(93127, resultSet.getInt(2));
        assertEquals("too many", resultSet.getString(3));

        // all ending nil values are trimmed
        resultSet.next();
        assertEquals(1, resultSet.getRowSize());

        dropSpace("basic_test");
    }

    @Test
    void testResultTraversal() {
        testHelper.executeLua(
            "box.schema.space.create('basic_test', {format={{name = 'id', type = 'integer'}}})",
            "box.space.basic_test:create_index('pk', { type = 'TREE', parts = {'id'} } )",
            "box.space.basic_test:insert{1}",
            "box.space.basic_test:insert{2}",
            "box.space.basic_test:insert{3}"
        );

        TarantoolResultSet resultSet = client.executeRequest(Requests.selectRequest("basic_test", "pk"));

        assertThrows(IllegalArgumentException.class, () -> resultSet.getInt(1));

        assertTrue(resultSet.next());
        assertEquals(1, resultSet.getInt(1));

        assertTrue(resultSet.next());
        assertEquals(2, resultSet.getInt(1));

        assertTrue(resultSet.next());
        assertEquals(3, resultSet.getInt(1));

        assertFalse(resultSet.next());

        assertTrue(resultSet.previous());
        assertEquals(2, resultSet.getInt(1));

        assertTrue(resultSet.previous());
        assertEquals(1, resultSet.getInt(1));

        assertFalse(resultSet.previous());

        dropSpace("basic_test");
    }

    @Test
    void testResultClose() throws IOException {
        testHelper.executeLua(
            "box.schema.space.create('basic_test', {format={{name = 'id', type = 'integer'}}})",
            "box.space.basic_test:create_index('pk', { type = 'TREE', parts = {'id'} } )",
            "box.space.basic_test:insert{1}",
            "box.space.basic_test:insert{2}",
            "box.space.basic_test:insert{3}"
        );

        TarantoolResultSet resultSet = client.executeRequest(Requests.selectRequest("basic_test", "pk"));

        resultSet.next();
        assertEquals(1, resultSet.getRowSize());

        resultSet.close();
        assertEquals(-1, resultSet.getRowSize());

        dropSpace("basic_test");
    }

    @Test
    void testGetEmptyResult() {
        testHelper.executeLua(
            "box.schema.space.create('basic_test', {format={{name = 'id', type = 'integer'}}})",
            "box.space.basic_test:create_index('pk', { type = 'TREE', parts = {'id'} } )"
        );

        TarantoolResultSet resultSet = client.executeRequest(Requests.selectRequest("basic_test", "pk"));

        assertTrue(resultSet.isEmpty());
        assertFalse(resultSet.next());

        dropSpace("basic_test");
    }

    @Test
    void testGetWrongColumn() {
        testHelper.executeLua(
            "box.schema.space.create('basic_test', {format={{name = 'id', type = 'integer'}}})",
            "box.space.basic_test:create_index('pk', { type = 'TREE', parts = {'id'} } )",
            "box.space.basic_test:insert{1}"
        );

        TarantoolResultSet resultSet = client.executeRequest(Requests.selectRequest("basic_test", "pk"));

        resultSet.next();
        assertThrows(IndexOutOfBoundsException.class, () -> resultSet.getByte(2));

        dropSpace("basic_test");
    }

    @Test
    void testGetByteValue() {
        testHelper.executeLua(
            "box.schema.space.create('byte_vals', { format = " +
                "{{name = 'id', type = 'integer'}, {name = 'byte_val', type = 'integer', is_nullable = true} }})",
            "box.space.byte_vals:create_index('pk', { type = 'TREE', parts = {'id'} } )",

            "box.space.byte_vals:insert{1, -128}",
            "box.space.byte_vals:insert{2, 127}",
            "box.space.byte_vals:insert{3, nil}",
            "box.space.byte_vals:insert{4, 0}",
            "box.space.byte_vals:insert{5, 15}",
            "box.space.byte_vals:insert{6, 114}",
            "box.space.byte_vals:insert{7, -89}",
            "box.space.byte_vals:insert{8, 300}",
            "box.space.byte_vals:insert{9, -250}"
        );

        TarantoolResultSet resultSet = client.executeRequest(Requests.selectRequest("byte_vals", "pk"));

        resultSet.next();
        assertEquals(-128, resultSet.getByte(2));

        resultSet.next();
        assertEquals(127, resultSet.getByte(2));

        // last nil value is trimmed
        resultSet.next();
        assertEquals(1, resultSet.getRowSize());

        resultSet.next();
        assertEquals(0, resultSet.getByte(2));
        assertFalse(resultSet.isNull(2));

        resultSet.next();
        assertEquals(15, resultSet.getByte(2));

        resultSet.next();
        assertEquals(114, resultSet.getByte(2));

        resultSet.next();
        assertEquals(-89, resultSet.getByte(2));

        resultSet.next();
        assertThrows(NotConvertibleValueException.class, () -> resultSet.getByte(2));

        resultSet.next();
        assertThrows(NotConvertibleValueException.class, () -> resultSet.getByte(2));

        dropSpace("byte_vals");
    }

    @Test
    void testGetShortValue() {
        testHelper.executeLua(
            "box.schema.space.create('short_vals', { format = " +
                "{{name = 'id', type = 'integer'}, {name = 'byte_val', type = 'integer', is_nullable = true} }" +
                "})",
            "box.space.short_vals:create_index('pk', { type = 'TREE', parts = {'id'} } )",

            "box.space.short_vals:insert{1, -32768}",
            "box.space.short_vals:insert{2, 32767}",
            "box.space.short_vals:insert{4, 0}",
            "box.space.short_vals:insert{5, -1}",
            "box.space.short_vals:insert{6, 12843}",
            "box.space.short_vals:insert{7, -7294}",
            "box.space.short_vals:insert{8, 34921}",
            "box.space.short_vals:insert{9, -37123}"
        );

        TarantoolResultSet resultSet = client.executeRequest(Requests.selectRequest("short_vals", "pk"));

        resultSet.next();
        assertEquals(-32768, resultSet.getShort(2));

        resultSet.next();
        assertEquals(32767, resultSet.getShort(2));

        resultSet.next();
        assertEquals(0, resultSet.getShort(2));
        assertFalse(resultSet.isNull(2));

        resultSet.next();
        assertEquals(-1, resultSet.getShort(2));

        resultSet.next();
        assertEquals(12843, resultSet.getShort(2));

        resultSet.next();
        assertEquals(-7294, resultSet.getShort(2));

        resultSet.next();
        assertThrows(NotConvertibleValueException.class, () -> resultSet.getShort(2));

        resultSet.next();
        assertThrows(NotConvertibleValueException.class, () -> resultSet.getShort(2));

        dropSpace("short_vals");
    }

    @Test
    void testGetIntValue() {
        testHelper.executeLua(
            "box.schema.space.create('int_vals', { format = " +
                "{{name = 'id', type = 'integer'}, {name = 'int_vals', type = 'integer', is_nullable = true} }" +
                "})",
            "box.space.int_vals:create_index('pk', { type = 'TREE', parts = {'id'} } )",

            "box.space.int_vals:insert{1, -2147483648}",
            "box.space.int_vals:insert{2, 2147483647}",
            "box.space.int_vals:insert{4, 0}",
            "box.space.int_vals:insert{5, -134}",
            "box.space.int_vals:insert{6, 589213}",
            "box.space.int_vals:insert{7, -1234987}",
            "box.space.int_vals:insert{8, 3897234258}",
            "box.space.int_vals:insert{9, -2289123645}"
        );

        TarantoolResultSet resultSet = client.executeRequest(Requests.selectRequest("int_vals", "pk"));

        resultSet.next();
        assertEquals(-2147483648, resultSet.getInt(2));

        resultSet.next();
        assertEquals(2147483647, resultSet.getInt(2));

        resultSet.next();
        assertEquals(0, resultSet.getInt(2));
        assertFalse(resultSet.isNull(2));

        resultSet.next();
        assertEquals(-134, resultSet.getInt(2));

        resultSet.next();
        assertEquals(589213, resultSet.getInt(2));

        resultSet.next();
        assertEquals(-1234987, resultSet.getInt(2));

        resultSet.next();
        assertThrows(NotConvertibleValueException.class, () -> resultSet.getInt(2));

        resultSet.next();
        assertThrows(NotConvertibleValueException.class, () -> resultSet.getInt(2));

        dropSpace("int_vals");
    }

    @Test
    void testGetLongValue() {
        testHelper.executeLua(
            "box.schema.space.create('long_vals', { format = " +
                "{{name = 'id', type = 'integer'}, {name = 'long_val', type = 'integer', is_nullable = true} }" +
                "})",
            "box.space.long_vals:create_index('pk', { type = 'TREE', parts = {'id'} } )",

            "box.space.long_vals:insert{1, -9223372036854775808LL}",
            "box.space.long_vals:insert{2, 9223372036854775807LL}",
            "box.space.long_vals:insert{3, 0LL}",
            "box.space.long_vals:insert{4, -89123LL}",
            "box.space.long_vals:insert{5, 2183428734598754LL}",
            "box.space.long_vals:insert{6, -918989823492348843LL}",
            "box.space.long_vals:insert{7, 18446744073709551615ULL}"
        );

        TarantoolResultSet resultSet = client.executeRequest(Requests.selectRequest("long_vals", "pk"));

        resultSet.next();
        assertEquals(-9223372036854775808L, resultSet.getLong(2));

        resultSet.next();
        assertEquals(9223372036854775807L, resultSet.getLong(2));

        resultSet.next();
        assertEquals(0, resultSet.getLong(2));
        assertFalse(resultSet.isNull(2));

        resultSet.next();
        assertEquals(-89123, resultSet.getLong(2));

        resultSet.next();
        assertEquals(2183428734598754L, resultSet.getLong(2));

        resultSet.next();
        assertEquals(-918989823492348843L, resultSet.getLong(2));

        resultSet.next();
        assertThrows(NotConvertibleValueException.class, () -> resultSet.getLong(2));

        dropSpace("long_vals");
    }

    @Test
    void testGetFloatValue() {
        testHelper.executeLua(
            "box.schema.space.create('float_vals', { format = " +
                "{{name = 'id', type = 'integer'}, {name = 'float_val', type = 'number', is_nullable = true} }" +
                "})",
            "box.space.float_vals:create_index('pk', { type = 'TREE', parts = {'id'} } )",

            "box.space.float_vals:insert{1, -12.6}",
            "box.space.float_vals:insert{2, 14.098}",
            "box.space.float_vals:insert{3, 0}",
            "box.space.float_vals:insert{4, -1230988}",
            "box.space.float_vals:insert{5, 2138562176}",
            "box.space.float_vals:insert{6, -78.0}",
            "box.space.float_vals:insert{7, 97.14827}"
        );

        TarantoolResultSet resultSet = client.executeRequest(Requests.selectRequest("float_vals", "pk"));

        resultSet.next();
        assertEquals(-12.6f, resultSet.getFloat(2));

        resultSet.next();
        assertEquals(14.098f, resultSet.getFloat(2));

        resultSet.next();
        assertEquals(0, resultSet.getFloat(2));

        resultSet.next();
        assertEquals(-1230988.0f, resultSet.getFloat(2));

        resultSet.next();
        assertEquals(2138562176.0f, resultSet.getFloat(2));

        resultSet.next();
        assertEquals(-78.0f, resultSet.getFloat(2));

        resultSet.next();
        assertEquals(97.14827f, resultSet.getFloat(2));

        dropSpace("float_vals");
    }

    @Test
    void testGetDoubleValue() {
        testHelper.executeLua(
            "box.schema.space.create('double_vals', { format = " +
                "{{name = 'id', type = 'integer'}, {name = 'double_val', type = 'number', is_nullable = true} }" +
                "})",
            "box.space.double_vals:create_index('pk', { type = 'TREE', parts = {'id'} } )",

            "box.space.double_vals:insert{1, -12.60}",
            "box.space.double_vals:insert{2, 43.9093}",
            "box.space.double_vals:insert{3, 0}",
            "box.space.double_vals:insert{4, -89234234}",
            "box.space.double_vals:insert{5, 532982423}",
            "box.space.double_vals:insert{6, -134.0}",
            "box.space.double_vals:insert{7, 4232.8264286}"
        );

        TarantoolResultSet resultSet = client.executeRequest(Requests.selectRequest("double_vals", "pk"));

        resultSet.next();
        assertEquals(-12.60, resultSet.getDouble(2));

        resultSet.next();
        assertEquals(43.9093, resultSet.getDouble(2));

        resultSet.next();
        assertEquals(0, resultSet.getDouble(2));

        resultSet.next();
        assertEquals(-89234234, resultSet.getDouble(2));

        resultSet.next();
        assertEquals(532982423, resultSet.getDouble(2));

        resultSet.next();
        assertEquals(-134.0f, resultSet.getDouble(2));

        resultSet.next();
        assertEquals(4232.8264286, resultSet.getDouble(2));

        dropSpace("double_vals");
    }

    @Test
    void testGetBooleanValue() {
        testHelper.executeLua(
            "box.schema.space.create('bool_vals', { format = " +
                "{{name = 'id', type = 'integer'}, {name = 'bool_val', type = 'boolean', is_nullable = true} }" +
                "})",
            "box.space.bool_vals:create_index('pk', { type = 'TREE', parts = {'id'} } )",

            "box.space.bool_vals:insert{1, true}",
            "box.space.bool_vals:insert{2, false}"
        );

        TarantoolResultSet resultSet = client.executeRequest(Requests.selectRequest("bool_vals", "pk"));

        resultSet.next();
        assertTrue(resultSet.getBoolean(2));

        resultSet.next();
        assertFalse(resultSet.getBoolean(2));

        dropSpace("bool_vals");
    }

    @Test
    void testGetBooleanFromNumber() {
        testHelper.executeLua(
            "box.schema.space.create('bool_vals', { format = " +
                "{{name = 'id', type = 'integer'}, {name = 'bool_val', type = 'number', is_nullable = true} }" +
                "})",
            "box.space.bool_vals:create_index('pk', { type = 'TREE', parts = {'id'} } )",

            "box.space.bool_vals:insert{1, 0}",
            "box.space.bool_vals:insert{2, 1}",
            "box.space.bool_vals:insert{3, 1.0}",
            "box.space.bool_vals:insert{4, 0.0}",
            "box.space.bool_vals:insert{5, -0.0}"
        );

        TarantoolResultSet resultSet = client.executeRequest(Requests.selectRequest("bool_vals", "pk"));

        resultSet.next();
        assertFalse(resultSet.getBoolean(2));

        resultSet.next();
        assertTrue(resultSet.getBoolean(2));

        resultSet.next();
        assertTrue(resultSet.getBoolean(2));

        resultSet.next();
        assertFalse(resultSet.getBoolean(2));

        resultSet.next();
        assertFalse(resultSet.getBoolean(2));

        dropSpace("bool_vals");
    }

    @Test
    void testGetBooleanFromString() {
        testHelper.executeLua(
            "box.schema.space.create('bool_vals', { format = " +
                "{{name = 'id', type = 'integer'}, {name = 'bool_val', type = 'string', is_nullable = true} }" +
                "})",
            "box.space.bool_vals:create_index('pk', { type = 'TREE', parts = {'id'} } )",

            "box.space.bool_vals:insert{1, '0'}",
            "box.space.bool_vals:insert{2, '1'}",
            "box.space.bool_vals:insert{3, 'true'}",
            "box.space.bool_vals:insert{4, 'false'}"
        );

        TarantoolResultSet resultSet = client.executeRequest(Requests.selectRequest("bool_vals", "pk"));

        resultSet.next();
        assertFalse(resultSet.getBoolean(2));

        resultSet.next();
        assertTrue(resultSet.getBoolean(2));

        resultSet.next();
        assertTrue(resultSet.getBoolean(2));

        resultSet.next();
        assertFalse(resultSet.getBoolean(2));

        dropSpace("bool_vals");
    }

    @Test
    void testGetBytesValue() {
        testHelper.executeLua(
            "box.schema.space.create('bin_vals', { format = " +
                "{{name = 'id', type = 'integer'}, {name = 'bin_val', type = 'scalar', is_nullable = true} }})",
            "box.space.bin_vals:create_index('pk', { type = 'TREE', parts = {'id'} } )",

            "box.space.bin_vals:insert{1, [[some text]]}",
            "box.space.bin_vals:insert{2, '\\01\\02\\03\\04\\05'}",
            "box.space.bin_vals:insert{3, 12}"
        );

        TarantoolResultSet resultSet = client.executeRequest(Requests.selectRequest("bin_vals", "pk"));

        resultSet.next();
        assertArrayEquals("some text".getBytes(StandardCharsets.UTF_8), resultSet.getBytes(2));

        resultSet.next();
        assertArrayEquals(new byte[] { 0x1, 0x2, 0x3, 0x4, 0x5 }, resultSet.getBytes(2));

        resultSet.next();
        assertThrows(NotConvertibleValueException.class, () -> resultSet.getBytes(2));

        dropSpace("bin_vals");
    }

    @Test
    void testGetStringValue() {
        testHelper.executeLua(
            "box.schema.space.create('string_vals', { format = " +
                "{{name = 'id', type = 'integer'}, {name = 'text_val', type = 'string', is_nullable = true} }" +
                "})",
            "box.space.string_vals:create_index('pk', { type = 'TREE', parts = {'id'} } )",

            "box.space.string_vals:insert{1, 'some text'}",
            "box.space.string_vals:insert{2, 'word'}",
            "box.space.string_vals:insert{3, ''}"
        );

        TarantoolResultSet resultSet = client.executeRequest(Requests.selectRequest("string_vals", "pk"));

        resultSet.next();
        assertEquals("some text", resultSet.getString(2));

        resultSet.next();
        assertEquals("word", resultSet.getString(2));

        resultSet.next();
        assertEquals("", resultSet.getString(2));

        dropSpace("string_vals");
    }

    @Test
    void testGetWrongStringValue() {
        TarantoolResultSet resultSet = client.executeRequest(Requests.evalRequest(
            "return {a=1,b=2}, {1,2,3}"
        ));

        resultSet.next();
        assertThrows(NotConvertibleValueException.class, () -> resultSet.getString(1));
        assertThrows(NotConvertibleValueException.class, () -> resultSet.getString(2));
    }

    @Test
    void testGetStringFromScalar() {
        testHelper.executeLua(
            "box.schema.space.create('scalar_vals', { format = " +
                "{{name = 'id', type = 'integer'}, {name = 'scalar_val', type = 'scalar', is_nullable = true} }" +
                "})",
            "box.space.scalar_vals:create_index('pk', { type = 'TREE', parts = {'id'} } )",

            "box.space.scalar_vals:insert{1, 'some text'}",
            "box.space.scalar_vals:insert{2, 12}",
            "box.space.scalar_vals:insert{3, 12.45}",
            "box.space.scalar_vals:insert{4, '\\01\\02\\03'}",
            "box.space.scalar_vals:insert{5, true}"
        );

        TarantoolResultSet resultSet = client.executeRequest(Requests.selectRequest("scalar_vals", "pk"));

        resultSet.next();
        assertEquals("some text", resultSet.getString(2));

        resultSet.next();
        assertEquals("12", resultSet.getString(2));

        resultSet.next();
        assertEquals("12.45", resultSet.getString(2));

        resultSet.next();
        assertEquals(new String(new byte[] { 0x1, 0x2, 0x3 }), resultSet.getString(2));

        resultSet.next();
        assertEquals("true", resultSet.getString(2));

        dropSpace("scalar_vals");
    }

    @Test
    void testGetObject() {
        testHelper.executeLua(
            "box.schema.space.create('object_vals', { format = " +
                "{{name = 'id', type = 'integer'}, {name = 'object_val', type = 'scalar', is_nullable = true} }" +
                "})",
            "box.space.object_vals:create_index('pk', { type = 'TREE', parts = {'id'} } )",

            "box.space.object_vals:insert{1, 'some text'}",
            "box.space.object_vals:insert{2, 12}",
            "box.space.object_vals:insert{3, 12.45}",
            "box.space.object_vals:insert{4, '\\01\\02\\03'}",
            "box.space.object_vals:insert{5, true}"
        );

        TarantoolResultSet resultSet = client.executeRequest(Requests.selectRequest("object_vals", "pk"));

        resultSet.next();
        assertEquals("some text", resultSet.getObject(2));

        resultSet.next();
        assertEquals(12, resultSet.getObject(2));

        resultSet.next();
        assertEquals(12.45, (double) resultSet.getObject(2));

        resultSet.next();
        assertEquals(new String(new byte[] { 0x1, 0x2, 0x3 }), resultSet.getObject(2));

        resultSet.next();
        assertTrue((boolean) resultSet.getObject(2));

        dropSpace("object_vals");
    }

    @Test
    void testGetBigInteger() {
        testHelper.executeLua(
            "box.schema.space.create('bigint_vals', { format = " +
                "{{name = 'id', type = 'integer'}, {name = 'bigint_val', type = 'integer', is_nullable = true} }" +
                "})",
            "box.space.bigint_vals:create_index('pk', { type = 'TREE', parts = {'id'} } )",

            "box.space.bigint_vals:insert{1, 10001}",
            "box.space.bigint_vals:insert{2, 12}",
            "box.space.bigint_vals:insert{3, 0}",
            "box.space.bigint_vals:insert{4, 18446744073709551615ULL}",
            "box.space.bigint_vals:insert{5, -4792}",
            "box.space.bigint_vals:insert{6, -9223372036854775808LL}"
        );

        TarantoolResultSet resultSet = client.executeRequest(Requests.selectRequest("bigint_vals", "pk"));

        resultSet.next();
        assertEquals(new BigInteger("10001"), resultSet.getBigInteger(2));

        resultSet.next();
        assertEquals(new BigInteger("12"), resultSet.getBigInteger(2));

        resultSet.next();
        assertEquals(new BigInteger("0"), resultSet.getBigInteger(2));

        resultSet.next();
        assertEquals(new BigInteger("18446744073709551615"), resultSet.getBigInteger(2));

        resultSet.next();
        assertEquals(new BigInteger("-4792"), resultSet.getBigInteger(2));

        resultSet.next();
        assertEquals(new BigInteger("-9223372036854775808"), resultSet.getBigInteger(2));

        dropSpace("bigint_vals");
    }

    @Test
    void testGetTuple() {
        testHelper.executeLua(
            "box.schema.space.create('animals', { format = " +
                "{{name = 'id', type = 'integer'}, " +
                " {name = 'name', type = 'string'}," +
                " {name = 'is_predator', type = 'boolean', is_nullable = true}}" +
                "})",
            "box.space.animals:create_index('pk', { type = 'TREE', parts = {'id'} } )",

            "box.space.animals:insert{1, 'Zebra', false}",
            "box.space.animals:insert{2, 'Lion', true}",
            "box.space.animals:insert{3, 'Monkey', nil}",
            "box.space.animals:insert{4, 'Wolf', true}",
            "box.space.animals:insert{5, 'Duck', nil}",
            "box.space.animals:insert{6, 'Raccoon', nil}"
        );

        int[] rowSizes = {3, 3, 2, 3, 2, 2};
        TarantoolResultSet resultSet = client.executeRequest(Requests.selectRequest("animals", "pk"));
        int rowNumber = 0;
        while (resultSet.next()) {
            TarantoolTuple tuple = resultSet.getTuple(TarantoolResultSet.ACTUAL_ROW_SIZE);
            assertEquals(rowSizes[rowNumber++], tuple.size());

            tuple = resultSet.getTuple(3);
            assertEquals(3, tuple.size());

            tuple = resultSet.getTuple(4);
            assertEquals(4, tuple.size());
            assertNull(tuple.get(4));
        }

        dropSpace("animals");
    }

    @Test
    void testGetTupleFromSizedSpace() {
        testHelper.executeLua(
            "box.schema.space.create('movies', {field_count = 4, format = " +
                "{{name = 'id', type = 'integer'}," +
                " {name = 'name', type = 'string'}," +
                " {name = 'genre', type = 'string', is_nullable = true}}" +
                "})",
            "box.space.movies:create_index('pk', { type = 'TREE', parts = {'id'} } )",

            "box.space.movies:insert{1, 'The Godfather', 'Drama', box.NULL}",
            "box.space.movies:insert{2, 'The Wizard of Oz', 'Fantasy', box.NULL}",
            "box.space.movies:insert{3, 'The Shawshank Redemption', nil, box.NULL}",
            "box.space.movies:insert{4, 'Pulp Fiction', 'Crime', box.NULL}",
            "box.space.movies:insert{5, '2001: A Space Odyssey', nil, box.NULL}",
            "box.space.movies:insert{6, 'Forrest Gump', nil, box.NULL}"
        );

        TarantoolResultSet resultSet = client.executeRequest(Requests.selectRequest("movies", "pk"));
        while (resultSet.next()) {
            TarantoolTuple tuple = resultSet.getTuple(TarantoolResultSet.ACTUAL_ROW_SIZE);
            assertEquals(4, tuple.size());

            tuple = resultSet.getTuple(4);
            assertEquals(4, tuple.size());
        }

        dropSpace("movies");
    }

    @Test
    void testRequestBoundTuple() {
        TarantoolResultSet resultSet = client.executeRequest(
            Requests.evalRequest(
                "return 'a', 'b', 'c', 'd', 'e'"
            )
        );
        resultSet.next();
        assertThrows(IllegalArgumentException.class, () -> resultSet.getTuple(-10));
        assertThrows(IllegalArgumentException.class, () -> resultSet.getTuple(4));
        assertEquals(5, resultSet.getTuple(5).size());
        assertEquals(5, resultSet.getTuple(TarantoolResultSet.ACTUAL_ROW_SIZE).size());

        TarantoolTuple tuple = resultSet.getTuple(100);
        assertEquals(100, tuple.size());
        assertEquals(100, tuple.toList().size());
        assertEquals(100, tuple.toArray().length);
        String[] values = {"a", "b", "c", "d", "e"};
        for (int i = 1; i <= 5; i++) {
            assertEquals(values[i - 1], tuple.get(i));
        }
        for (int i = 6; i <= 100; i++) {
            assertNull(tuple.get(i));
        }
        assertThrows(IndexOutOfBoundsException.class, () -> tuple.get(101));
    }

    @Test
    void testGetList() {
        TarantoolResultSet resultSet = client.executeRequest(
            Requests.evalRequest("return {'a','b','c'}, {1,2,3}, nil, {}, 'string'")
        );

        resultSet.next();
        assertEquals(5, resultSet.getRowSize());
        assertEquals(Arrays.asList("a", "b", "c"), resultSet.getList(1));
        assertEquals(Arrays.asList(1, 2, 3), resultSet.getList(2));
        assertNull(resultSet.getList(3));
        assertEquals(Collections.emptyList(), resultSet.getList(4));
        assertThrows(NotConvertibleValueException.class, () -> resultSet.getList(5));
    }

    @Test
    void testGetMap() {
        TarantoolResultSet resultSet = client.executeRequest(
            Requests.evalRequest(
                "return {a=1,b=2}, {['key 1']=8,['key 2']=9}, {['+']='add',['/']='div'}, 'string'"
            )
        );

        resultSet.next();
        assertEquals(4, resultSet.getRowSize());
        assertEquals(
            new HashMap<Object, Object>() {
                {
                    put("a", 1);
                    put("b", 2);
                }
            },
            resultSet.getMap(1));
        assertEquals(
            new HashMap<Object, Object>() {
                {
                    put("key 1", 8);
                    put("key 2", 9);
                }
            },
            resultSet.getMap(2));
        assertEquals(
            new HashMap<Object, Object>() {
                {
                    put("+", "add");
                    put("/", "div");
                }
            },
            resultSet.getMap(3));
        assertThrows(NotConvertibleValueException.class, () -> resultSet.getMap(4));
    }

    @Test
    void testGetMixedValues() {
        testHelper.executeLua(
            "function getMixed() return 10, -20.5, 'string', nil, true, {1,2,3}, {x='f',y='t'} end"
        );
        TarantoolResultSet resultSet = client.executeRequest(Requests.callRequest("getMixed"));

        resultSet.next();
        assertEquals(7, resultSet.getRowSize());
        assertEquals(10, resultSet.getInt(1));
        assertEquals(-20.5, resultSet.getDouble(2));
        assertEquals("string", resultSet.getString(3));
        assertTrue(resultSet.isNull(4));
        assertTrue(resultSet.getBoolean(5));
        assertEquals(Arrays.asList(1, 2, 3), resultSet.getList(6));
        assertEquals(
            new HashMap<Object, Object>() {
                {
                    put("x", "f");
                    put("y", "t");
                }
            },
            resultSet.getMap(7));
    }

    @Test
    void testGetNullValues() {
        testHelper.executeLua(
            "function getNull() return nil end"
        );
        TarantoolResultSet resultSet = client.executeRequest(Requests.callRequest("getNull"));

        resultSet.next();
        assertFalse(resultSet.getBoolean(1));
        assertEquals(0, resultSet.getByte(1));
        assertEquals(0, resultSet.getShort(1));
        assertEquals(0, resultSet.getInt(1));
        assertEquals(0, resultSet.getLong(1));
        assertNull(resultSet.getBigInteger(1));
        assertEquals(0.0, resultSet.getFloat(1));
        assertEquals(0.0, resultSet.getDouble(1));
        assertNull(resultSet.getList(1));
        assertNull(resultSet.getMap(1));
        assertNull(resultSet.getObject(1));
        assertNull(resultSet.getString(1));
        assertNull(resultSet.getBytes(1));
    }

    @Test
    void testGetFloatNumberFromString() {
        testHelper.executeLua(
            "function getString() return '120.5' end"
        );
        TarantoolResultSet resultSet = client.executeRequest(Requests.callRequest("getString"));

        resultSet.next();
        assertEquals(120.5f, resultSet.getFloat(1));
        assertEquals(120.5d, resultSet.getDouble(1));
    }

    void testGetIntegerNumberFromString() {
        testHelper.executeLua(
            "function getString() return '120' end"
        );
        TarantoolResultSet resultSet = client.executeRequest(Requests.callRequest("getString"));

        resultSet.next();
        assertEquals(120, resultSet.getByte(1));
        assertEquals(120, resultSet.getShort(1));
        assertEquals(120, resultSet.getInt(1));
        assertEquals(120, resultSet.getLong(1));
        assertEquals(BigInteger.valueOf(120), resultSet.getBigInteger(1));
    }


    @Test
    void testGetNotParsableNumberFromString() {
        testHelper.executeLua(
            "function getString() return 'five point six' end"
        );
        TarantoolResultSet resultSet = client.executeRequest(Requests.callRequest("getString"));

        resultSet.next();
        assertThrows(NotConvertibleValueException.class, () -> resultSet.getByte(1));
        assertThrows(NotConvertibleValueException.class, () -> resultSet.getShort(1));
        assertThrows(NotConvertibleValueException.class, () -> resultSet.getInt(1));
        assertThrows(NotConvertibleValueException.class, () -> resultSet.getLong(1));
        assertThrows(NotConvertibleValueException.class, () -> resultSet.getBigInteger(1));
        assertThrows(NotConvertibleValueException.class, () -> resultSet.getFloat(1));
        assertThrows(NotConvertibleValueException.class, () -> resultSet.getDouble(1));
    }

    @Test
    void testGetTooLargeNumberValue() {
        testHelper.executeLua(
            "function getNumber() return 300, 100000, 5000000000, 10000000000000000000ULL end",
            "function getString() return '300', '100000', '5000000000', '10000000000000000000' end"
        );
        TarantoolResultSet numberResult = client.executeRequest(Requests.callRequest("getNumber"));

        numberResult.next();
        assertThrows(NotConvertibleValueException.class, () -> numberResult.getByte(1));
        assertThrows(NotConvertibleValueException.class, () -> numberResult.getShort(2));
        assertThrows(NotConvertibleValueException.class, () -> numberResult.getInt(3));
        assertThrows(NotConvertibleValueException.class, () -> numberResult.getLong(4));

        TarantoolResultSet stringResult = client.executeRequest(Requests.callRequest("getString"));

        stringResult.next();
        assertThrows(NotConvertibleValueException.class, () -> stringResult.getByte(1));
        assertThrows(NotConvertibleValueException.class, () -> stringResult.getShort(2));
        assertThrows(NotConvertibleValueException.class, () -> stringResult.getInt(3));
        assertThrows(NotConvertibleValueException.class, () -> stringResult.getLong(4));
    }

    private void dropSpace(String spaceName) {
        testHelper.executeLua(String.format("box.space.%1$s and box.space.%1$s:drop()", spaceName));
    }
}
