<a href="http://tarantool.org">
   <img src="https://avatars2.githubusercontent.com/u/2344919?v=2&s=250"
align="right">
</a>

# Java connector for Tarantool 1.7.4+

[![Coverage Status][coveralls-badge]][coveralls-page]

[coveralls-badge]: https://coveralls.io/repos/github/tarantool/tarantool-java/badge.svg?branch=master
[coveralls-page]: https://coveralls.io/github/tarantool/tarantool-java?branch=master

[![Join the chat at https://gitter.im/tarantool/tarantool-java](https://badges.gitter.im/Join%20Chat.svg)](https://gitter.im/tarantool/tarantool-java?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge)

To get the Java connector for Tarantool 1.6.9, visit
[this GitHub page](https://github.com/tarantool/tarantool-java/tree/connector-1.6.9).

## Table of contents
* [Getting started](#getting-started)
* [Where to get help](#where-to-get-help)

## Getting started

1. Add a dependency to your `pom.xml` file.

```xml
<dependency>
  <groupId>org.tarantool</groupId>
  <artifactId>connector</artifactId>
  <version>1.7.4</version>
</dependency>
```

2. Configure `TarantoolClientConfig`.

```java
TarantoolClientConfig config = new TarantoolClientConfig();
config.username = "test";
config.password = "test";
```

3. Implement your `SocketChannelProvider`.
   It should return a connected `SocketChannel`.

```java
SocketChannelProvider socketChannelProvider = new SocketChannelProvider() {
           @Override
           public SocketChannel get(int retryNumber, Throwable lastError) {
               if (lastError != null) {
                   lastError.printStackTrace(System.out);
               }
               try {
                   return SocketChannel.open(new InetSocketAddress("localhost", 3301));
               } catch (IOException e) {
                   throw new IllegalStateException(e);
               }
           }
       };
```

Here you could also implement some reconnection or fallback policy.
Remember that `TarantoolClient` adopts a
[fail-fast](https://en.wikipedia.org/wiki/Fail-fast) policy
when a client is not connected.

The `TarantoolClient` will stop functioning if your implementation of a socket
channel provider raises an exception or returns a null. You will need a new
instance of client to recover. Hence, you should only throw in case you have
met unrecoverable error.

Below is an example of `SocketChannelProvider` implementation that handles short
tarantool restarts.

```java
SocketChannelProvider socketChannelProvider = new SocketChannelProvider() {
    @Override
    public SocketChannel get(int retryNumber, Throwable lastError) {
        long deadline = System.currentTimeMillis() + RESTART_TIMEOUT;
        while (!Thread.currentThread().isInterrupted()) {
            try {
                return SocketChannel.open(new InetSocketAddress("localhost", 3301));
            } catch (IOException e) {
                if (deadline < System.currentTimeMillis())
                    throw new RuntimeException(e);
                try {
                    Thread.sleep(100);
                } catch (InterruptedException ignored) {
                    Thread.currentThread().interrupt();
                }
            }
        }
        throw new RuntimeException(new TimeoutException("Connect timed out."));
    }
};
```

4. Create a client.

```java
TarantoolClient client = new TarantoolClientImpl(socketChannelProvider, config);
```

> **Notes:**
> * `TarantoolClient` is thread-safe and asynchronous, so you should use one
>   client inside the whole application.
> * `TarantoolClient` does not support name resolution for fields, indexes,
>   spaces and so on. We highly recommend to use server-side Lua when working
>   with named items. For example, you could create a data access object (DAO)
>   with simple CRUD functions. If, for some reason, you do need client name
>   resolution, you could create a function that returns necessary name-to-ID
>   mappings.

`TarantoolClient` provides four interfaces to execute queries:

* `SyncOps` - returns the operation result
* `AsyncOps` - returns the operation result as a `Future`
* `ComposableAsyncOps` - return the operation result as a `CompletionStage`
* `FireAndForgetOps` - returns the query ID

Feel free to override any method of `TarantoolClientImpl`. For example, to hook
all the results, you could override this:

```java
protected void complete(long code, CompletableFuture<?> q);
```

## Spring NamedParameterJdbcTemplate usage example

To configure sockets you should implements SQLSocketProvider and add socketProvider=abc.xyz.MySocketProvider to connect url.
For example tarantool://localhost:3301?user=test&password=test&socketProvider=abc.xyz.MySocketProvider

```java
NamedParameterJdbcTemplate template = new NamedParameterJdbcTemplate(new DriverManagerDataSource("tarantool://localhost:3301?user=test&password=test"));
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

For more implementation details, see [API documentation](http://tarantool.github.io/tarantool-java/apidocs/index.html).

## Where to get help

Got problems or questions? Post them on
[Stack Overflow](http://stackoverflow.com/questions/ask/advice) with the
`tarantool` and `java` tags, or use these tags to search the existing knowledge
base for possible answers and solutions.

## Building
To run tests
```
./mvnw clean test
```
