<a href="http://tarantool.org">
   <img src="https://avatars2.githubusercontent.com/u/2344919?v=2&s=250"
align="right">
</a>

# Java connector for Tarantool 1.7.4+

[![Join the chat at https://gitter.im/tarantool/tarantool-java](https://badges.gitter.im/Join%20Chat.svg)](https://gitter.im/tarantool/tarantool-java?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge)
[![Maven Central](https://maven-badges.herokuapp.com/maven-central/org.tarantool/connector/badge.svg)](https://maven-badges.herokuapp.com/maven-central/org.tarantool/connector)
[![Build Status](https://travis-ci.org/tarantool/tarantool-java.svg?branch=master)](https://travis-ci.org/tarantool/tarantool-java)
[![Coverage Status](https://coveralls.io/repos/github/tarantool/tarantool-java/badge.svg?branch=master)](https://coveralls.io/github/tarantool/tarantool-java?branch=master)

To get the Java connector for Tarantool 1.6.9, visit
[this GitHub page](https://github.com/tarantool/tarantool-java/tree/connector-1.6.9).

## Table of contents
* [Getting started](#getting-started)
* [JDBC](#JDBC)
* [Cluster support](#cluster-support)
* [Where to get help](#where-to-get-help)

## Getting started

1. Add a dependency to your `pom.xml` file:

```xml
<dependency>
  <groupId>org.tarantool</groupId>
  <artifactId>connector</artifactId>
  <version>1.9.1</version>
</dependency>
```

2. Configure `TarantoolClientConfig`:

```java
TarantoolClientConfig config = new TarantoolClientConfig();
config.username = "test";
config.password = "test";
```

3. Create a client:

```java
TarantoolClient client = new TarantoolClientImpl("host:3301", config);
```

using `TarantoolClientImpl(String, TarantoolClientConfig)` is equivalent to:

```java
SocketChannelProvider socketChannelProvider = new SingleSocketChannelProviderImpl("host:3301")
TarantoolClient client = new TarantoolClientImpl(socketChannelProvider, config);
```

You could implement your own `SocketChannelProvider`. It should return 
a connected `SocketChannel`. Feel free to implement `get(int retryNumber, Throwable lastError)`
using your appropriate strategy to obtain the channel. The strategy can take into
account current attempt number (retryNumber) and the last transient error occurred on
the previous attempt.

The `TarantoolClient` will be closed if your implementation of a socket
channel provider raises exceptions. However, throwing a `SocketProviderTransientException`
or returning `null` value are handled by the client as recoverable errors. In these cases,
the client will make next attempt to obtain the socket channel. Otherwise, you will need
a new instance of client to recover. Hence, you should only throw an error different
to `SocketProviderTransientException` in case you have met unrecoverable error.

Below is an example of `SocketChannelProvider` implementation that tries
to connect no more than 3 times, two seconds for each attempt at max.

```java
SocketChannelProvider socketChannelProvider = new SocketChannelProvider() {
    @Override
    public SocketChannel get(int retryNumber, Throwable lastError) {
        if (retryNumber > 3) {
            throw new RuntimeException("Too many attempts");
        }
        SocketChannel channel = null;
        try {
            channel = SocketChannel.open();
            channel.socket().connect(new InetSocketAddress("localhost", 3301), 2000);
            return channel;
        } catch (IOException e) {
            if (channel != null) {
                 try {
                     channel.close();
                 } catch (IOException ignored) { }
            }
            throw new SocketProviderTransientException("Couldn't connect to server", e);
        }
    }
};
```

Same behaviour can be achieved using built-in `SingleSocketChannelProviderImpl`:

```java
TarantoolClientConfig config = new TarantoolClientConfig();
config.connectionTimeout = 2_000; // two seconds timeout per attempt
config.retryCount = 3;            // three attempts at max

SocketChannelProvider socketChannelProvider = new SingleSocketChannelProviderImpl("localhost:3301")
TarantoolClient client = new TarantoolClientImpl(socketChannelProvider, config);
```

`SingleSocketChannelProviderImpl` implements `ConfigurableSocketChannelProvider` that
makes possible for the client to configure a socket provider.

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
protected void complete(TarantoolPacket packet, TarantoolOp<?> future);
```

### Client config options

The client configuration options are represented through the `TarantoolClientConfig` class.

Supported options are follow:

1. `username` is used to authenticate and authorize an user in a Taratool server instance.
   Default value is `null` that means client will attempt to auth as a *guest*.
2. `password` is used to authenticate an user in a Taratool server instance.
   Default value is `null`.
3. `defaultRequestSize` used to be an initial binary buffer size in bytes to send requests.
   Default value is `4096` (4 KB).
4. `predictedFutures` is used to initialize an initial capacity of hash map which stores
   response futures. The client is asynchronous under the hood even though it provides
   a synchronous operations using `java.concurrent.CompletableFuture`.
   Default value is `(1024 * 1024) / 0.75) + 1`.
5. `writerThreadPriority` describes a priority of writer thread.
   Default value is `Thread.NORM_PRIORITY` (5).
6. `readerThreadPriority` describes a priority of reader thread.
   Default value is `Thread.NORM_PRIORITY` (5).
7. `sharedBufferSize` sets a shared buffer size in bytes (place where client collects
   requests when socket is busy on write).
   Default value is `8 * 1024 * 1024` (8 MB).
8. `directWriteFactor` is used as a factor to calculate a threshold whether
   request will be accommodated in the shared buffer. If the request size exceeds
   `directWriteFactor * sharedBufferSize` request is sent directly.
   Defualt value is `0.5`.
9. `writeTimeoutMillis` sets the max time in ms to perform writing and send the bytes.
    Default value is 60 * 1000 (1 minute).
10. `useNewCall` configures whether client has to use new *CALL* request signature or old
    one used to be active in Tarantool 1.6.
    Default value is `true`.
11. `initTimeoutMillis` sets a max time in ms to establish connection to the server
    Default values is `60 * 1000L` (1 minute).
12. `connectionTimeout` is a hint and can be passed to the socket providers which
    implement `ConfigurableSocketChannelProvider` interface. This hint should be
    interpreter as a connection timeout in ms per attempt where `0` means no limit.
    This options restricts a time budget to perform one connection attempt, while
    `initTimeoutMillis` limits an overall time to obtain a connection.
    Default value is `2 * 1000` (2 seconds).
13. `retryCount` is a hint and can be passed to the socket providers which
    implement `ConfigurableSocketChannelProvider` interface. This hint should be
    interpreter as a maximal number of attempts to connect to Tarantool instance.
    Default value is `3`.  
14. `operationExpiryTimeMillis` is a default request timeout in ms.
    Default value is `1000` (1 second).

## Spring NamedParameterJdbcTemplate usage example

The JDBC driver uses `TarantoolClient` implementation to provide a communication with server.
To configure socket channel provider you should implements SocketChannelProvider and add
`socketChannelProvider=abc.xyz.MySocketChannelProvider` to connect url.

For example:

```
jdbc:tarantool://localhost:3301?user=test&password=test&socketProvider=abc.xyz.MySocketProvider
```

Here is an example how you can use the driver covered by Spring `DriverManagerDataSource`:

```java
NamedParameterJdbcTemplate template = new NamedParameterJdbcTemplate(new DriverManagerDataSource("jdbc:tarantool://localhost:3301?user=test&password=test"));
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

## JDBC

### Batch updates

`Statement` and `PreparedStatement` objects can be used to submit batch
updates.

For instance, using `Statement` object:

```java
Statement statement = connection.createStatement();
statement.addBatch("INSERT INTO student VALUES (30, 'Joe Jones')");
statement.addBatch("INSERT INTO faculty VALUES (2, 'Faculty of Chemistry')");
statement.addBatch("INSERT INTO student_faculty VALUES (30, 2)");

int[] updateCounts = stmt.executeBatch();
```

or using `PreparedStatement`:

```java
PreparedStatement stmt = con.prepareStatement("INSERT INTO student VALUES (?, ?)");
stmt.setInt(1, 30);
stmt.setString(2, "Michael Korj");
stmt.addBatch();
stmt.setInt(1, 40);
stmt.setString(2, "Linda Simpson");
stmt.addBatch();

int[] updateCounts = stmt.executeBatch();
```

The connector uses a pipeliing when it performs a batch request. It means
each query is asynchronously sent one-by-one in order they were specified
in the batch.

There are a couple of caveats:

- JDBC spec recommends that *auto-commit* mode should be turned off
to prevent the driver from committing a transaction when a batch request
is called. The connector is not support transactions and *auto-commit* is
always enabled, so each statement from the batch is executed in its own
transaction.

- DDL operations aren't transactional in Tarantool. In this way, a batch
like this can produce an undefined behaviour (i.e. second statement can fail
with an error that `student` table does not exist).

```java
statement.addBatch("CREATE TABLE student (id INT PRIMARY KEY, name VARCHAR(100))");
statement.addBatch("INSERT INTO student VALUES (1, 'Alex Smith')");
```

- If `vinyl` storage engine is used an execution order of batch statements is
not specified. __NOTE:__ This behaviour is incompatible with JDBC spec in the
sentence "Batch commands are executed serially (at least logically) in the
order in which they were added to the batch"

- The driver continues processing the remaining commands in a batch once execution
of a command fails.

## Cluster support

To be more fault-tolerant the connector provides cluster extensions. In
particular `TarantoolClusterClient` and built-in `RoundRobinSocketProviderImpl`
used as a default `SocketProvider` implementation. When currently connected
instance is down then the client will try to reconnect to the first available
instance using strategy defined in a socket provider. You need to supply
a list of nodes which will be used by the cluster client to provide such
ability. Also you can prefer to use a [discovery mechanism](#auto-discovery)
in order to dynamically fetch and apply the node list.

### The RoundRobinSocketProviderImpl class

This cluster-aware provider uses addresses pool to connect to DB server.
The provider picks up next address in order the addresses were passed.

Similar to `SingleSocketChannelProviderImpl` this RR provider also
relies on two options from the config: `TarantoolClientConfig.connectionTimeout`
and `TarantoolClientConfig.retryCount` but in a bit different way.
The latter option says how many times the provider should try to establish a
connection to _one instance_ before failing an attempt. The provider requires
positive retry count to work properly. The socket timeout is used to limit
an interval between connections attempts per instance. In other words, the provider
follows a pattern _connection should succeed after N attempts with M interval between
them at max_. 

### Basic cluster client usage

1. Configure `TarantoolClusterClientConfig`:

```java
TarantoolClusterClientConfig config = new TarantoolClusterClientConfig();
// fill other settings
config.operationExpiryTimeMillis = 2000;
config.executor = Executors.newSingleThreadExecutor();
```

2. Create an instance of `TarantoolClusterClientImpl`. You need to provide
an initial list of nodes:

```java
String[] nodes = new String[] { "myHost1:3301", "myHost2:3302", "myHost3:3301" };
TarantoolClusterClient client = new TarantoolClusterClient(config, nodes);
``` 

3. Work with the client using same API as defined in `TarantoolClient`:

```java
client.syncOps().insert(23, Arrays.asList(1, 1));
```

### Auto-discovery

Auto-discovery feature allows a cluster client to fetch addresses of 
cluster nodes to reflect changes related to the cluster topology. To achieve
this you have to create a Lua function on the server side which returns 
a single array result. Client periodically polls the server to obtain a 
fresh list and apply it if its content changes.

1. On the server side create a function which returns nodes:

```bash
tarantool> function get_cluster_nodes() return { 'host1:3301', 'host2:3302', 'host3:3301' } end
```

You need to pay attention to a function contract we are currently supporting:
* The client never passes any arguments to a discovery function.
* A discovery function _must_ return an array of strings (i.e `return {'host1:3301', 'host2:3301'}`).
* Each string _should_ satisfy the following pattern `host[:port]`
  (or more strictly `/^[^:]+(:\d+)?$/` - a mandatory host containing any string
  and an optional colon followed by digits of the port). Also, the port must be
  in a range between 1 and 65535 if one is presented.
* A discovery function _may_ return multi-results but the client takes
  into account only first of them (i.e. `return {'host:3301'}, discovery_delay`, where 
  the second result is unused). Even more, any extra results __are reserved__ by the client
  in order to extend its contract with a backward compatibility.
* A discovery function _should NOT_ return no results, empty result, wrong type result,
  and Lua errors. The client discards such kinds of results but it does not affect the discovery
  process for next scheduled tasks. 

2. On the client side configure discovery settings in `TarantoolClusterClientConfig`:

```java
TarantoolClusterClientConfig config = new TarantoolClusterClientConfig();
// fill other settings
config.clusterDiscoveryEntryFunction = "get_cluster_nodes"; // discovery function used to fetch nodes 
config.clusterDiscoveryDelayMillis = 60_000;                // how often client polls the discovery server  
```

3. Create a client using the config made above.

```java
TarantoolClusterClient client = new TarantoolClusterClient(config);
client.syncOps().insert(45, Arrays.asList(1, 1));
```

### Auto-discovery caveats

* You need to set _not empty_ value to `clusterDiscoveryEntryFunction` to enable auto-discovery feature.
* There are only two cases when a discovery task runs: just after initialization of the cluster
  client and a periodical scheduler timeout defined in `TarantoolClusterClientConfig.clusterDiscoveryDelayMillis`. 
* A discovery task always uses an active client connection to get the nodes list.
  It's in your responsibility to provide a function availability as well as a consistent
  nodes list on all instances you initially set or obtain from the task.
* Every address which is unmatched with `host[:port]` pattern will be filtered out from
  the target addresses list.
* If some error occurs while a discovery task is running then this task
  will be aborted without any after-effects for next task executions. These cases, for instance, are 
  a wrong function result (see discovery function contract) or a broken connection. 
  There is an exception if the client is closed then discovery process will stop permanently.
* It's possible to obtain a list which does NOT contain the node we are currently
  connected to. It leads the client to try to reconnect to another node from the 
  new list. It may take some time to graceful disconnect from the current node.
  The client does its best to catch the moment when there are no pending responses
  and perform a reconnection.  

### Client config options

In addition to the options for [the standard client](#client-config-options), cluster
config provides some extra options:

1. `executor` defines an executor that will be used as a thread of execution to retry writes.
   Default values is `null` which means the cluster client will use *a single thread executor*.
2. `clusterDiscoveryEntryFunction` is a name of the stored function to be used to fetch list of
   instances.
   Default value is `null` (not set).
3. `clusterDiscoveryDelayMillis` describes how often in ms to poll the server for a new list of
   cluster nodes.
   Default value is `60 * 1000` (1 minute).

## Where to get help

Got problems or questions? Post them on
[Stack Overflow](http://stackoverflow.com/questions/ask/advice) with the
`tarantool` and `java` tags, or use these tags to search the existing knowledge
base for possible answers and solutions.

## Building

To run unit tests use:
 
```bash
./mvnw clean test
``` 

To run integration tests use:

```bash
./mvnw clean verify
```
