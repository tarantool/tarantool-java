<a href="http://tarantool.org">
   <img src="https://avatars2.githubusercontent.com/u/2344919?v=2&s=250"
align="right">
</a>

# Java connector for Tarantool 1.7.4+

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

`TarantoolClient` provides three interfaces to execute queries:

* `SyncOps` - returns the operation result
* `AsyncOps` - returns the operation result as a `Future`
* `FireAndForgetOps` - returns the query ID

Feel free to override any method of `TarantoolClientImpl`. For example, to hook
all the results, you could override this:

```java
protected void complete(long code, FutureImpl<List> q);
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