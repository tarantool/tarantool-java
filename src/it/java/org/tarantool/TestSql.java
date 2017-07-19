//package org.tarantool;
//
//import java.io.IOException;
//import java.net.InetSocketAddress;
//import java.net.Socket;
//import java.net.URI;
//import java.sql.ResultSet;
//import java.sql.SQLException;
//import java.util.Arrays;
//import java.util.Collections;
//import java.util.LinkedHashMap;
//import java.util.Map;
//import java.util.Properties;
//
//import org.springframework.jdbc.core.RowMapper;
//import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
//import org.springframework.jdbc.datasource.DriverManagerDataSource;
//import org.tarantool.jdbc.SQLSocketProvider;
//
//public class TestSql {
//
//    public static class TestSocketProvider implements SQLSocketProvider {
//
//        @Override
//        public Socket getConnectedSocket(URI uri, Properties params) {
//            Socket socket;
//            socket = new Socket();
//            try {
//                socket.connect(new InetSocketAddress(params.getProperty("host","localhost"), Integer.parseInt(params.getProperty("port", "3301"))));
//            } catch (Exception e) {
//                throw new RuntimeException("Couldn't connect to tarantool using" + params, e);
//            }
//            return socket;
//        }
//    }
//
//    public static void main(String[] args) throws IOException, SQLException {
//
//        NamedParameterJdbcTemplate template = new NamedParameterJdbcTemplate(new DriverManagerDataSource("tarantool://localhost:3301?username=test&password=test&socketProvider=org.tarantool.TestSql$TestSocketProvider"));
//        RowMapper<Object> rowMapper = new RowMapper<Object>() {
//            @Override
//            public Object mapRow(ResultSet resultSet, int i) throws SQLException {
//                return Arrays.asList(resultSet.getInt(1), resultSet.getString(2));
//            }
//        };
//
//        try {
//            System.out.println(template.update("drop table hello_world", Collections.<String, Object>emptyMap()));
//        } catch (Exception ignored) {
//        }
//        System.out.println(template.update("create table hello_world(hello int not null PRIMARY KEY, world varchar(255) not null)", Collections.<String, Object>emptyMap()));
//        Map<String, Object> params = new LinkedHashMap<String, Object>();
//        params.put("text", "hello world");
//        params.put("id", 1);
//
//        System.out.println(template.update("insert into hello_world(hello, world) values(:id,:text)", params));
//        System.out.println(template.query("select * from hello_world", rowMapper));
//
//        System.out.println(template.query("select * from hello_world where hello=:id", Collections.singletonMap("id", 1), rowMapper));
//    }
//}
