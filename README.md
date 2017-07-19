
First you should add snaphost repository and  dependency to your pom file
```xml
<dependency>
  <groupId>org.tarantool</groupId>
  <artifactId>connector</artifactId>
  <version>1.8.jdbc-SNAPSHOT</version>
</dependency>
```


## Spring NamedParameterJdbcTemplate usage example.

To configure sockets you should implements SQLSocketProvider and add socketProvider=abc.xyz.MySocketProvider to connect url. 
For example tarantool://localhost:3301?username=test&password=test&socketProvider=abc.xyz.MySocketProvider

```java
             NamedParameterJdbcTemplate template = new NamedParameterJdbcTemplate(new DriverManagerDataSource("tarantool://localhost:3301?username=test&password=test"));
             RowMapper<Object> rowMapper = new RowMapper<Object>() {
                 @Override
                 public Object mapRow(ResultSet resultSet, int i) throws SQLException {
                     return Arrays.asList(resultSet.getInt(1), resultSet.getString(2));
                 }
             };
     
             try {
                 System.out.println(template.update("drop table hello_world", Collections.<String, Object>emptyMap()));
             } catch (Exception ignored) {
             }
             System.out.println(template.update("create table hello_world(hello int not null PRIMARY KEY, world varchar(255) not null)", Collections.<String, Object>emptyMap()));
             Map<String, Object> params = new LinkedHashMap<String, Object>();
             params.put("text", "hello world");
             params.put("id", 1);
     
             System.out.println(template.update("insert into hello_world(hello, world) values(:id,:text)", params));
             System.out.println(template.query("select * from hello_world", rowMapper));
     
             System.out.println(template.query("select * from hello_world where hello=:id", Collections.singletonMap("id", 1), rowMapper));
```

