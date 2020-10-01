<a href="http://tarantool.org">
   <img src="https://avatars2.githubusercontent.com/u/2344919?v=2&s=250"
align="right">
</a>

# Java connector for Tarantool 1.7.4+

[![Maven Central](https://maven-badges.herokuapp.com/maven-central/org.tarantool/connector/badge.svg)](https://maven-badges.herokuapp.com/maven-central/org.tarantool/connector)
[![Build Status](https://travis-ci.org/tarantool/tarantool-java.svg?branch=master)](https://travis-ci.org/tarantool/tarantool-java)
[![Coverage Status](https://coveralls.io/repos/github/tarantool/tarantool-java/badge.svg?branch=master)](https://coveralls.io/github/tarantool/tarantool-java?branch=master)

To get the Java connector for Tarantool 1.6.9, visit
[this GitHub page](https://github.com/tarantool/tarantool-java/tree/connector-1.6.9).

## Table of contents
* [Getting started](#getting-started)
* [Spring NamedParameterJdbcTemplate usage example](#spring-namedparameterjdbctemplate-usage-example)
* [JDBC](#JDBC)
* [Cluster support](#cluster-support)
* [Getting a result](#getting-a-result)
* [Logging](#logging)
* [Building](#building)
* [Where to get help](#where-to-get-help)

## Getting started

1. Add a dependency to your `pom.xml` file (look for a last version on the
   [releases page](https://github.com/tarantool/tarantool-java/releases)):

```xml
<dependency>
  <groupId>org.tarantool</groupId>
  <artifactId>connector</artifactId>
  <version>1.9.4</version>
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
protected void complete(TarantoolPacket packet, CompletableFuture<?> future);
```

### Supported operation types

Given a tarantool space as:

```lua
box.schema.space.create('cars', { format =
    { {name='id', type='integer'},"
      {name='name', type='string'},"
      {name='max_mph', type='integer'} }
});
box.space.cars:create_index('pk', { type='TREE', parts={'id'} });
box.space.cars:create_index('speed_idx', { type='TREE', unique=false, parts={'max_mph', type='unsigned'} });
``` 

and a stored function as well:

```lua
function getVehiclesSlowerThan(max_mph, max_size)
    return box.space.cars.index.speed_idx:select(max_mph, {iterator='LT', limit=max_size});
end;
```

Let's have a look what sort of operations we can apply to it using a connector. 
*Note*: assume Tarantool generated id equal `512` for the newly created `cars` space.

* SELECT (find tuples matching the search pattern)

For instance, we can get a single tuple by id like 

```java
ops.select(512, 0, Collections.singletonList(1), 0, 1, Iterator.EQ);
```

or using more readable lookup names

```java
ops.select("cars", "pk", Collections.singletonList(1), 0, 1, Iterator.EQ);
```

or even using builder-way to construct a query part-by-part

```java
import static org.tarantool.dsl.Requests.selectRequest;

ops.execute(
    selectRequest("cars", "pk")
        .iterator(Iterator.EQ)
        .limit(1)
);
```

* INSERT (put a tuple in the space)

Let's insert a new tuple into the space 

```java
ops.insert(512, Arrays.asList(1, "Lada Niva", 81));
```

do the same using names

```java
ops.insert("cars", Arrays.asList(1, "Lada Niva", 81));
```

or using DSL

```java
import static org.tarantool.dsl.Requests.insertRequest;

ops.execute(
    insertRequest("cars", Arrays.asList(1, "Lada Niva", 81))
);
```

* REPLACE (insert a tuple into the space or replace an existing one)

The syntax is quite similar to insert operation

```java
import static org.tarantool.dsl.Requests.replaceRequest;

ops.replace(512, Arrays.asList(2, "UAZ-469", 60));
ops.replace("cars", Arrays.asList(2, "UAZ-469", 60));
ops.execute(
    replaceRequest("cars", Arrays.asList(2, "UAZ-469", 60))
);
```

* UPDATE (update a tuple)

Let's modify one of existing tuples

```java
ops.update(512, Collections.singletonList(1), Arrays.asList("=", 1, "Lada 4×4"));
```

Lookup way:

```java
ops.update("cars", Collections.singletonList(1), Arrays.asList("=", 1, "Lada 4×4"));
```

or using DSL

```java
import static org.tarantool.dsl.Operations.assign;
import static org.tarantool.dsl.Requests.updateRequest;

ops.execute(
    updateRequest("cars", Collections.singletonList(1), assign(1, "Lada 4×4"))
);
```

*Note*: since Tarantool 2.3.x you can refer to tuple fields by name:

```java
ops.update(512, Collections.singletonList(1), Arrays.asList("=", "name", "Lada 4×4"));
```

* UPSERT (update a tuple if it exists, otherwise try to insert it as a new tuple)

An example looks as a mix of both insert and update operations:

```java
import static org.tarantool.dsl.Operations.assign;
import static org.tarantool.dsl.Requests.upsertRequest;

ops.upsert(512, Collections.singletonList(3), Arrays.asList(3, "KAMAZ-65224", 65), Arrays.asList("=", 2, 65));
ops.upsert("cars", Collections.singletonList(3), Arrays.asList(3, "KAMAZ-65224", 65), Arrays.asList("=", 2, 65));
ops.execute(
    upsertRequest("cars", Collections.singletonList(3), assign(2, 65))
);
``` 

*Note*: since Tarantool 2.3.x you can refer to tuple fields by name:

```java
ops.upsert("cars", Collections.singletonList(3), Arrays.asList(3, "KAMAZ-65224", 65), Arrays.asList("=", "max_mph", 65));
```

* DELETE (delete a tuple)

Remove a tuple using one of the following forms:

```java
import static org.tarantool.dsl.Requests.deleteRequest;

ops().delete(512, Collections.singletonList(1));
// same via lookup
ops().delete("cars", Collections.singletonList(1));
// same via DSL
ops.execute(deleteRequest("cars", Collections.singletonList(1)));
```

* CALL / CALL v1.6 (call a stored function)

Let's invoke the predefined function to fetch slower enough vehicles:

```java
import static org.tarantool.dsl.Requests.callRequest;

ops().call("getVehiclesSlowerThan", 80, 10);
// same via DSL
ops.execute(callRequest("getVehiclesSlowerThan").arguments(80, 10));
```

*NOTE*: to use obsolete Tarantool v1.6 operation, configure it as follows:

```java
ops().setCallCode(Code.OLD_CALL);
ops().call("getVehiclesSlowerThan", 80, 10);
// same via DSL
ops.execute(callRequest("getVehiclesSlowerThan").arguments(80, 10).useCall16(true));
```  \

* EVAL (evaluate a Lua expression)

To evaluate expressions using Lua, you can invoke the following operation:

```java
import static org.tarantool.dsl.Requests.evalRequest;

ops().eval("return getVehiclesSlowerThan(...)", 90, 50);
// same via DSL
ops.execute(evalRequest("return getVehiclesSlowerThan(...)")).arguments(90, 50));
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
   Default value is `0.5`.
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

## String space/index resolution

Each operation that requires space or index to be executed, can work with
number ID as well as string name of a space or an index.
Assume, we have `my_space` space with space ID `512` and its primary index
`primary` with index ID `0`. Then, for instance, `select` operations can be
performed using their names:

```java
client.syncOps().select(512, 0, Collections.singletonList(1), 0, 1, Iterator.EQ);
// or using more convenient way
client.syncOps().select("my_space", "primary", Collections.singletonList(1), 0, 1, Iterator.EQ);
```

Because _iproto_ has not yet supported string spaces and indexes, a client caches current server
schema in memory. The client relies on protocol SCHEMA_ID and sends each request with respect to
cached schema version. The schema is used primarily to resolve string names of spaces or indexes
against its integer IDs.

### Schema update

1. Just after a (re-)connection to the Tarantool instance.
   The client cannot guarantee that new instance is the same and has same schema,
   thus, the client drops the cached schema and fetches new one.
2. Receiving a schema version error as a response to our request.
   It's possible some request can be rejected by server because of schema
   mismatching between client and server. In this case the schema will be
   reloaded and the refused request will be resent using the updated schema
   version.
3. Sending a DDL request and receiving a new version in a response.
4. Sending a request against a non-existent space/index name.
   The client cannot exactly know whether name was not found because of
   it does not exist or it has not the latest schema version. A ping request
   is sent in the case to check a schema version and then a client will reload
   it if needed. The original request will be retried if a space / an index
   name will be found in a new schema.

### Schema support caveats

1. Each schema reloading requires at least two extra requests to fetch spaces and
   indexes metadata respectively. There is also a ping request followed by reloading
   of the schema to check whether the client has outdated version (see point 4 in
   [Schema update](#schema-update)).
2. In some circumstance, requests can be rejected several times until both client's
   and server's versions matches. It may take significant amount of time or even be
   a cause of request timeout.
3. The client guarantees an order of synchronous requests per thread. Other cases such
   as asynchronous or multi-threaded requests may be out of order before the execution.

## Getting a result

Traditionally, when a response is parsed by the internal MsgPack implementation the client
will return it as a heterogeneous list of objects `List` that in most cases is inconvenient
for users to use. It requires a type guessing as well as a writing more boilerplate code to work
with typed data. Most of the methods which are provided by `TarantoolClientOps` (i.e. `select`)
return raw de-serialized data via `List`. 

Consider a small example how it is usually used:

```java
// get an untyped array of tuples
List<?> result = client.syncOps().execute(Requests.selectRequest("space", "pk"));
for (int i = 0; i < result.size(); i++) {
    // get the first tuple (also untyped)
    List<?> row = result.get(i);
    // try to cast the first tuple as a couple of values
    int id = (int) row.get(0);
    String text = (String) row.get(1);
    processEntry(id, text);
}
```

There is an additional way to work with data using `TarantoolClient.executeRequest(TarantoolRequestConvertible)`
method. This method returns a result wrapper over original data that allows to extract in a more
typed manner rather than it is directly provided by MsgPack serialization. The `executeRequest`
returns the `TarantoolResultSet` which provides a bunch of methods to get data. Inside the result
set the data is represented as a list of rows (tuples) where each row has columns (fields).
In general, it is possible that different rows have different size of their columns in scope of
the same result. 

```java
TarantoolResultSet result = client.executeRequest(Requests.selectRequest("space", "pk"));
while (result.next()) {
    long id = result.getLong(0);
    String text = result.getString(1);
    processEntry(id, text);
}
```

The `TarantoolResultSet` provides an implicit conversation between types if it's possible.

Numeric types internally can represent each other if a type range allows to do it. For example,
byte 100 can be represented as a short, int and other types wider than byte. But 200 integer
cannot be narrowed to a byte because of overflow (byte range is [-128..127]). If a floating
point number is converted to a integer then the fraction part will be omitted. It is also
possible to convert a valid string to a number. 

Boolean type can be obtained from numeric types such as byte, short, int, long, BigInteger,
float and double where 1 (1.0) means true and 0 (0.0) means false. Or it can be got from
a string using well-known patterns such as "1", "t|true", "y|yes", "on" for true and
"0", "f|false", "n|no", "off" for false respectively.

String type can be converted from a byte array and any numeric types. In case of `byte[]`
all bytes will be interpreted as a UTF-8 sequence.  

There is a special method called `getObject(int, Map)` where a user can provide its own
mapping functions to be applied if a designated type matches a value one.

For instance, using the following map each strings will be transformed to an upper case and
boolean values will be represented as strings "yes" or "no":

```java
Map<Class<?>, Function<Object, Object>> mappers = new HashMap<>();
mappers.put(
    String.class,
    v -> ((String) v).toUpperCase()
);
mappers.put(
    Boolean.class,
    v -> (boolean) v ? "yes" : "no"
);
``` 

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

The connector uses a pipelining when it performs a batch request. It means
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

### Cluster client config options

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

## Logging

The connector uses its own logging facade to abstract from any logging libraries
which can be used inside the apps where the connector attached. At the moment,
the facade supports JUL as a default logging system, SLF4J facade, and Logback
directly via SLF4J interface.

### Logging integration

The logging facade offers several ways in integrate its internal logging with foreign one in order:

* Using system property `org.tarantool.logging.provider`. Supported values are *jdk* and *slf4j*
  for the java util logging and SLF4J/Logback respectively. For instance, use 
  `java -Dorg.tarantool.logging.provider=slf4j <...>`.

* Using Java SPI mechanism. Implement your own provider org.tarantool.logging.LoggerProvider
  To register your provider save `META-INF.services/org.tarantool.logging.LoggerProvider` file
  with a single line text contains a fully-qualified class name of the provider implementation.

```bash
cat META-INF/services/org.tarantool.logging.LoggerProvider
org.mydomain.MySimpleLoggerProvider
```

* CLASSPATH exploring. Now, the connector will use SLF4J if Logback is also in use. 

* If nothing is successful JUL will be used by default.

### Supported loggers

| Logger name                                    | Level | Description                                       |
| ---------------------------------------------- | ----- | ------------------------------------------------- |
| o.t.c.TarantoolClusterStoredFunctionDiscoverer | WARN  | prints out invalid discovery addresses            |
| o.t.TarantoolClusterClient                     | TRACE | prints out request retries after transient errors |
| o.t.TarantoolClientImpl                        | WARN  | prints out reconnect issues                       |

## Building

To run unit tests use:
 
```bash
./mvnw clean test
``` 

To run integration tests use:

```bash
./mvnw clean verify
```

## Where to get help

Got problems or questions? Post them on
[Stack Overflow](http://stackoverflow.com/questions/ask/advice) with the
`tarantool` and `java` tags, or use these tags to search the existing knowledge
base for possible answers and solutions.
