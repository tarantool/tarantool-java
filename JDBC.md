# Tarantool JDBC driver

## Contents

- [Tarantool JDBC driver](#tarantool-jdbc-driver)
  - [Contents](#contents)
  - [1. Introduction](#1-introduction)
  - [2. Getting started](#2-getting-started)
    - [Getting the driver](#getting-the-driver)
    - [Configuring the class path](#configuring-the-class-path)
    - [Preparing the Tarantool database](#preparing-the-tarantool-database)
  - [3. Initializing the driver](#3-initializing-the-driver)
    - [Using JDBC API](#using-jdbc-api)
    - [Loading the driver](#loading-the-driver)
    - [Connecting to the database](#connecting-to-the-database)
      - [Supported connection parameters](#supported-connection-parameters)
    - [Retrieving general database information](#retrieving-general-database-information)
    - [Using the driver in the enterprise environment](#using-the-driver-in-the-enterprise-environment)
      - [Supported DataSource properties](#supported-datasource-properties)
    - [Closing the connection](#closing-the-connection)
    - [Understanding the Tarantool connection](#understanding-the-tarantool-connection)
  - [4. Working with the database](#4-working-with-the-database)
    - [Issuing a query](#issuing-a-query)
    - [Exploring a result columns info](#exploring-a-result-columns-info)
    - [Processing a result](#processing-a-result)
      - [Closing a result set](#closing-a-result-set)
      - [Getting auto-generated values](#getting-auto-generated-values)
      - [Batching updates](#batching-updates)
    - [Closing a statement](#closing-a-statement)
    - [Using escape syntax](#using-escape-syntax)
      - [Scalar functions](#scalar-functions)
      - [Outer joins](#outer-joins)
      - [LIKE escape characters](#like-escape-characters)
      - [Limiting returned rows escape](#limiting-returned-rows-escape)

## 1. Introduction

Java Database Connectivity (JDBC) is a specification that defines an API (set of interfaces) for the programs written in
Java. It defines how a client may access a database. It is part of the Java Standard Edition platform and provides
methods to query and update data in a database.

Since version 2, Tarantool database started supporting relational database features including SQL. Thus, some of the
JDBC features became available to be implemented on the connector side.

Java connector partly implements JDBC 4.2 standard and allows Java programs to connect to a Tarantool 2.x database using
pure Java code. The connector is Type-4 driver written in pure Java, and uses a native network protocol to communicate
with a Tarantool server.

This guide was mainly created to show the basic usage of the driver, highlight and explain some features that may work
in a bit different way than expected or do not work at all because of the Tarantool NoSQL nature or current Tarantool
SQL limitations. By the way, more details and examples can be found inside the JavaDoc API and the JDBC specification.

References:
1. [JDBC Specification](https://download.oracle.com/otndocs/jcp/jdbc-4_2-mrel2-spec/index.html),
1. [JDBC API](https://docs.oracle.com/javase/8/docs/api/java/sql/package-summary.html),
1. [Tarantool SQL Syntax](https://www.tarantool.io/en/doc/2.2/reference/reference_sql/sql/),
1. [Tarantool SQL Binary Protocol](https://www.tarantool.io/en/doc/2.2/dev_guide/internals/sql_protocol/).

## 2. Getting started

### Getting the driver

There are two options how the driver can be obtained. The first way is to add the driver as a project dependency
using build tools such as Maven or Gradle:

Maven (`pom.xml`):

```xml
<dependency>
  <groupId>org.tarantool</groupId>
  <artifactId>connector</artifactId>
  <version>1.9.2</version>
</dependency>
```

Gradle (`build.gradle`):

```groovy
dependencies {
    compile group: 'org.tarantool', name: 'connector', version: '1.9.2'
}
```

Please visit the [releases page](https://github.com/tarantool/tarantool-java/releases) to find the latest connector
version.

Alternatively, if you need to use a precompiled driver you can download a JAR file from Maven using a direct link (for
instance, [connector-1.9.2.jar](https://repo1.maven.org/maven2/org/tarantool/connector/1.9.2/connector-1.9.2.jar)).

Finally, if you want to make changes to the source code and use a modified version then you should clone [the official
connector repository](https://github.com/tarantool/tarantool-java), apply your patches, and build the driver. To build
the driver it's required a prepared environment as a follows: Java 1.8 or above and Maven 3. Also, the connector source
code is bundled with a Maven wrapper that helps to locally download a proper version of Maven to be used to make a
connector.

Build the project:

```shell script
$ cd tarantool-java
$ mvn clean package
```

or using the provided wrapper

```shell script
$ cd tarantool-java
$ ./mvnw clean package
```

The final JAR file will be placed at `/target` folder according to the Maven conventions.

References:
1. [Tarantool Java connector](https://github.com/tarantool/tarantool-java).

### Configuring the class path

To start to use the driver in your application the driver JAR file has to be present in java CLASSPATH. It can be
achieved a couple of ways, for instance, setting a CLASSPATH environment variable or using `-cp` (--classpath) option of
`java` command.

Let's set a CLASSPATH variable in context of the running JVM:

```shell script
CLASSPATH=./MyApp.jar:./connector-1.9.2.jar:. java MyApp
```

References:
1. [Setting the class path by Oracle](https://docs.oracle.com/javase/8/docs/technotes/tools/windows/classpath.html).

### Preparing the Tarantool database

Because Java does not support unix sockets as a transport the Tarantool server must be configured to allow TCP/IP
connections. To allow connections you need to configure it properly using a configuration file or execute commands
directly on the database.

Configure the server to listen to 3301 port on localhost:

```lua
box.cfg {
    listen = 3301
}
```

Once you have made sure the server is correctly listening for TCP/IP connections the next step is to verify that users
are allowed to connect to the server.

Let's change the password for the predefined user `admin` that has `super` role:

```lua
box.schema.user.passwd('admin', 'p@$$vv0rd')
```

We will use `admin` user later.

References:
1. [Tarantool instance configuration](https://www.tarantool.io/en/doc/2.2/book/admin/instance_config/),
1. [Tarantool database access control](https://www.tarantool.io/en/doc/2.2/book/box/authentication/).

## 3. Initializing the driver

### Using JDBC API

In most cases, in order to use JDBC, an application should import the interfaces from the `java.sql` package:

```java
import java.sql.Connection;
import java.sql.Statement;
```

However, it's possible to import Tarantool JDBC interfaces that extend standard JDBC ones and provide extra incompatible
capabilities. If you want to use those extensions you can add lines like:

```java
import org.tarantool.jdbc.TarantoolConnection;
import org.tarantool.jdbc.TarantoolStatement;
```

**Note**: Be careful using vendor specific interfaces because they may decrease application portability of yours. It is
always a good idea to import JDBC interfaces instead.

### Loading the driver

The Tarantool driver implements JDBC 4.2 that introduces the driver SPI (Service Provider Interface). It allows to load
the driver class automatically when the JAR file in your classpath.

However, often external tools or frameworks (that can work with JDBC drivers) require you to exactly specify a driver
class name. In this case, you need to choose `org.tarantool.jdbc.SQLDriver`.

### Connecting to the database

The Tarantool driver registers under JDBC sub-protocol called `tarantool`. The driver accepts the following connection
string:

```text
jdbc:tarantool://[user[:password]@][host[:port]][?param1=value1[&param2=value2...]]
```

where

* `user` is an optional username to be used to connect. The default username is `guest`.
* `password` is an optional password defined for a user. The default value is an empty string.
* `host` is an optional hostname of the database. The default value is `localhost`.
* `port` is an optional database port. The default value is `3301`.

To connect, you need to get a `Connection` instance from JDBC. To do this, you use the `DriverManager.getConnection()`
method. Let's establish a new connection using the database info we've configured earlier
[here](#preparing-the-tarantool-database).

```java
Connection connection = DriverManager.getConnection(
    "jdbc:tarantool://localhost:3301", "admin", "p@$$vv0rd"
);
```

**Note**: Tarantool connections aren't pooled and they don't share any resources such as TCP/IP connection. Each
invocation of `DriverManager.getConnection()` returns a new established connection. But, in most cases you don't need to
have several connections in your application because of the internal asynchronous nature of a connection (for more
details see [Understanding the Tarantool connection](#understanding-the-tarantool-connection)).

References:
1. [java.sql.DriverManager](https://docs.oracle.com/javase/8/docs/api/java/sql/DriverManager.html).

#### Supported connection parameters

In addition, the driver supports a number of properties which can be used to specify additional driver behaviour
specific to Tarantool. These properties may be specified in either the connection URL or an additional `Properties`
object parameter to `DriverManager.getConnection()`.

* **host** (string) - Tarantool server host. Defaults to `localhost`;
* **port** (integer) - Tarantool server port. Defaults to `3301`;
* **socketChannelProvider** (string) - socket channel provider class that implements
  `org.tarantool.SocketChannelProvider`. Defaults to `org.tarantool.SingleSocketChannelProviderImpl`;
* **user** (string) - username to connect to the database. The default value is `guest`;
* **password** (string) - user's password to connect to the database. The default value is unset;
* **loginTimeout** (integer) - number of milliseconds to wait for a connection establishment. Defaults to 60000 (a
  minute);
* **queryTimeout** (integer) - number of milliseconds to wait before a timeout is occurred for the query. The default
  value is 0 (infinite).

The following examples illustrate the use of the methods to establish a connection.

Using an `Properties` instance:

```java
String url = "jdbc:tarantool://localhost";
Properties props = new Properties();
props.setProperty("user", "admin");
props.setProperty("password", "strong-password");
props.setProperty("port", "3301");
Connection connection = DriverManager.getConnection(url, props);
```

Using the embedded URL string options:

```java
String url = "jdbc:tarantool://admin:strong-password@localhost:3301";
Connection connection = DriverManager.getConnection(url);
```

Or using URL query parameters:

```java
String url = "jdbc:postgresql://localhost?port=3301&user=admin&password=strong-password";
Connection connection = DriverManager.getConnection(url);
```

If a property is specified both in URL main part (before `?` sign) and in URL query part (after `?` sign), the value
from main part is ignored.

If a property is specified both in URL and in `Properties` object, the value from `Properties` takes precedence.

References:
1. [java.sql.Connection](https://docs.oracle.com/javase/8/docs/api/java/sql/Connection.html).

### Retrieving general database information

To provide information about the underlying data source you can use the `DatabaseMetaData` that is implemented by this
driver. It includes more than 150 methods which help you to find out the common info about the datasource (username,
connection URL, database product version etc.), what database features are supported, what datasource limits are set,
what SQL objects and their attributes exist (tables, columns, functions and so on), and transaction support offered by
the datasource as well.

Let's try to explore some properties and objects of the database we've connected to:

```java
DatabaseMetaData databaseMeta = connection.getMetadata();
String url = databaseMeta.getURL();
boolean areBatchesSupported = databaseMeta.supportsBatchUpdates();
boolean areTransactionsSupported = databaseMeta.supportsTransactions();
List<String> tables = new ArrayList<>();
try (ResultSet resultSet = databaseMeta.getTables()) {
    while (resultSet.next()) {
        tables.add(resultSet.getString("TABLE_NAME"));
    }
}

// let's assume we have an util method to somehow display discovered properties
Utils.printGeneralDatasourceInfo(
    url, areBatchesSupported, areTransactionsSupported, tables
);
```

**Note:** Not all of the database metadata methods provide meaningful info now. Please check the supported methods on
the *Tarantool JDBC status* page.

References:
1. [javax.sql.DatabaseMetaData](https://docs.oracle.com/javase/8/docs/api/java/sql/DatabaseMetaData.html).
1. [Tarantool JDBC status](https://github.com/tarantool/tarantool-java/wiki/JDBC-status).

### Using the driver in the enterprise environment

The enterprise environment (i.e. JavaEE) often uses `javax.sql` package to configure and manage connections, distributed
transactions and so on. The Tarantool driver provides an implementation of `DataSource`
(`org.tarantool.jdbc.ds.SQLDataSource`) to be more compatible with JDBC specification.

Usually, JDBC drivers implement connection pooling under the `DataSource` abstraction (including
`ConnectionPoolDataSource` and `PooledConnection`) to increase performance of applications. However, the Tarantool
driver does not do pooling because of using an asynchronous approach to send the request that allows to reuse the same
connection by different threads (see [Understanding the Tarantool connection](#understanding-the-tarantool-connection)).

For example, a data source can be instantiated directly:

```java
SQLDataSource source = new SQLDataSource();
source.setDataSourceName("Tarantool Datasource");
source.setServerName("localhost");
source.setPortNumber(3301);
source.setUser("admin");
source.setPassword("string-password");

Connection connection = source.getConnection();
```

References:
1. [javax.sql.DataSource](https://docs.oracle.com/javase/8/docs/api/javax/sql/DataSource.html).

#### Supported `DataSource` properties

Most of the datasource properties repeat the respective connection parameters (see
[Supported connection parameters](#supported-connection-parameters)).

* **serverName** (string) - Tarantool server host. Defaults to `localhost`;
* **portNumber** (integer) - Tarantool server port. Defaults to `3301`;
* **socketChannelProvider** (string) - socket channel provider class that implements
  `org.tarantool.SocketChannelProvider`. Defaults to `org.tarantool.SingleSocketChannelProviderImpl`;
* **user** (string) - username to connect to the database. The default value is `guest`;
* **password** (string) - user's password to connect to the database. The default value is unset;
* **description** (read-only, string) - vendor specific definition of the data source;
* **datasourceName** (string) - data source name;
* **loginTimeout** (integer) - number of milliseconds to wait for a connection establishment. Defaults to 60000 (a
  minute);
* **queryTimeout** (integer) - number of milliseconds to wait before a timeout is occurred for the query. The default
  value is 0 (infinite).

### Closing the connection

It usually should be avoided to close the connection manually because of it is done by your application container. But
if you want to shutdown the connection you need to use `Connection.close()` method. Once you close the connection all
the associated resources such as statements and result sets will be automatically released. Most of the their methods
will became unable to invoke anymore.

### Understanding the Tarantool connection

The implementation of Tarantool JDBC connection uses asynchronous mechanism under the hood to execute queries via
`org.tarantool.TarantoolCliemtImpl`. The connection registers a request obtaining a future object as a immediate
response and blocks the caller thread awaiting the result on this future. Once the future is resolved the connection
unblocks the thread, keeps the final result and makes it available to the caller side.

Usually, a JDBC connection uses a synchronous commuication protocol and cannot be shared between several thread at the
same time. It makes sense if you have a multi-threaded environment and expect parallel query execution per each thread.
To overcome this, it is often implemented the connection pools where a thread has to acquire one available connection to
be used and release it back to the pool after the work with it. This technique improves performance but it does not
scale up well under a variable workload. Moreover, this solution requires multiple connections be open that holds
additional system resources.

On the other hand, asynchronous nature of communication between the driver and Tarantool instances makes possible to
issue queries from several threads being not blocked to each other on awaiting responses using the same connection
instance. In this way, one connection object can shared between multiple threads which can work with this connection
simultaneously and do not block each other. Thus, you do not need to use several connections in your app because of no
valuable impaction on performance (at least, in scope of one Tarantool datasource).

But that approach may cause undefined order of the request executions, if you try to execute queries using same
connection from the different threads. So, the serial execution is guaranteed only for requests made from one thread.

## 4. Working with the database

### Issuing a query

To issue SQL statements to the database, you require a `Statement` or `PreparedStatement` instance. Once you have the
statement, you can use it to retrieve or modify database data.

The data query often called DQL (Data Query Language) and usually represented through `SELECT` SQL syntax and will
return a `ResultSet` object, which contains the entire result. You can perform such queries using, for instance,
`Statement.executeQuery(String)` or `PreparedStatement.exeuteQuery()`. Both should return a result set satisfying to
the query or raise an error if the result set cannot be returned.

The modification queries usually consists of DDL (Data Definition Language, where typical commands are `CREATE`,
`ALTER`, `DROP` and so on) and DML (Data Manipulation Language, with `INSERT`, `UPDATE`, `DELETE` group of commands)
statements. Such statements always return a count of *objects* affected (depends on a query type) or 0 for SQL
statements that return nothing. Using `Statement.executeUpdate(String)` or `PreparedStatement.executeUpdate()` you can
perform updates and receive an update count of objects processed or raise an error if such count cannot be returned.

If the type of query is unknown until run time it possible to use `Statement.execute(String)` or
`PreparedStatement.execute()` that return a flag indicating result type returned. The method `execute` returns *true* if
the first result is a `ResultSet` object and *false* if it is an update count. Then `Statement.getResultSet()` or
`Statement.getUpdateCount()` methods can be called to obtain an appropriate type of data. The `execute()` methods are
also used when it needs to retrieve multiple result sets per request using `Statement.getMoreResults()`. Tarantool does
not support that sort of response per one request now, however.

Let's fetch all too expensive employees at a company:

```java
List<String> emails = new ArrayList<>();
try (
    Statement statement = connection.createStatement();
    ResultSet resultSet = statement.executeQuery(
        "SELECT e.email FROM employee e WHERE e.salary > 1000"
    );
) {
    while (resultSet.next()) {
        emails.add(resultSet.getString(1));
    }
}
sendMail(emails, "Heya, you are fired!");
```

or do the same using `PreparedStatement`:

```java
PreparedStatement statement = connection.prepareStatement(
    "SELECT e.email FROM employee e WHERE e.salary > ?"
);
statement.setInt(1, 1000);

List<String> emails = new ArrayList<>();
try (ResultSet resultSet = statement.executeQuery()) {
    while (resultSet.next()) {
        emails.add(resultSet.getString(1));
    }
}
sendMail(emails, "Heya, you are fired!");

statement.close();
```

Let's add some rare animal species to the Red Book to see how we can process modification queries:

```java
try (
    Statement statement = connection.createStatement();
) {
    int count = statement.executeUpdate(
        "UPDATE animal SET in_redlist = true " +
        "WHERE name = 'Spoon-billed sandpiper'"
    );
    if (count > 0) {
        ...
    }
}
```

with `PreparedStatement`:

```java
PreparedStatement statement = connection.prepareStatement(
    "UPDATE animal SET in_redlist = ? WHERE name = ?"
);
statement.setBoolean(1, true);
statement.setString(2, "Spoon-billed sandpiper");

int rows = statement.executeUpdate();
if (count > 0) {
    ...
}

statement.close();
```

Usually, the `Statement` is used for simple single-shot queries that do not contain variable parts (query parameters).
And there is `PreparedStatement` that supports input parameters (placeholders marked by `?` sign) and can be
pre-compiled once and be reused multiple times with different parameters.

The most valuable benefits of using `PreparedStatement` over `Statement` can be described as a follows:

**Query parameters**. `PreparedStatement` supports parameter bindings as well as convenient methods to work with them.
It allows to write a query once and execute the query multiple times passing non-constant values. It also brings a
positive effect on protection against SQL injections because of using pre-complied statement structure and strict typed
API to set values (see `PreparedStatement.setXXX()` family methods). On the other hand, using `Statement`, if you need a
parametrized query, you have to build it up manually and be careful not making a mistake while a query construction.

**Query pre-processing**. The driver can perform some extra preparations over a query, say translate *escape syntax*,
before it will be sent to the database. Because `PreparedStatement` is designed to be prepared once and be reused after,
it can positively impacts on performance avoiding redundant operations in the future.

**Query cache**. Usually, each query being sent to the database is parsed and cached on the server side to improve query
execution performance. `PreparedStatement` uses a constant query string that makes easier to be cached by the database.
Parameters are passed separately and are not under consideration by the query cache. `Statement` has to inject
parameters as a part of query string that leads the query to be considered as a different key from the cache
perspective.

**Server prepared statement**. Tarantool server provides *prepared statements* since 2.3.1 that allows to pre-compile
and cache statements on the server side. In this case, the driver once prepare a query string it uses the corresponding
statement ID number received from the database to perform further queries. Next, each time a JDBC statement issues
a query, it sends *statement ID* instead of the SQL text over a network that positively reduces packet payload as well
as time of possible parsing and compiling the sent query on the database side. For more details, see
[Understanding prepared statements](#understanding-prepared-statements). 

**Note:** The third statement type named `CallableStatement` is unsupported by the driver at the moment.

Finally, the statements support the following properties related to the result sets handling:

As it was mentioned above, the statements may produce multiple result sets per a query via `execute()`. To manage all
the returned result sets you can use `Statement.getMoreResults(int)` method that receive one of the following constants:
`Statement.CLOSE_CURRENT_RESULT`, `Statement.KEEP_CURRENT_RESULT`, or `Statement.CLOSE_ALL_RESULTS`. They controls
whether a previous result set should be automatically closed when a next one is acquired. Tarantool does not allow to
execute several queries in one request, so it is not possible to obtain several result sets as result of one query and
so there is no reason to use those methods. However for the sake of compatibility with the standard the driver
technically supports `getMoreResults()` without an argument and with `Statement.CLOSE_CURRENT_RESULT` argument (other
two constants are not supported).

`Statement.NO_GENERATED_KEYS` and `Statement.RETURN_GENERATED_KEYS` options. The first option works by default and
it is usually used when you do not want to receive the generated values. The second option relies on the support
on the server side and allow to receive only _auto-generated_ values for the primary keys. The generated IDs can be
received via `Statement.getGeneratedKeys()` that returns a result set with a single integer column named
`GENERATED_KEY` (see [Getting auto-generated values](#getting-auto-generated-values) for more details).

`Statement.setMaxRows(int)` may be used to limit amount of rows a result set can have (the rest is discarded). This is
implemented on the driver side for compliance with JDBC standard and does not allow to reduce traffic transmitted over a
network and amount of memory consumed for a result set on the driver side.

References:
1. [java.sql.Statement](https://docs.oracle.com/javase/8/docs/api/java/sql/Statement.html).
1. [java.sql.PreparedStatement](https://docs.oracle.com/javase/8/docs/api/java/sql/PreparedStatement.html).

#### Understanding prepared statements

To execute the same (parameterized) statement repeatedly without recompilation overhead, Tarantool provides support for
the *server prepared statement*. This feature allows clients to pre-compile their queries on the database side and use a
*statement ID* as a reference to the prepared statement instead of the original SQL query text.

The JDBC driver can use two different strategies when `PreparedStatement` is used. It depends on support for prepared
statements by the server. If the driver connects to a Tarantool instance being below than 2.3.1 (incapable to prepare
statements) an PreparedStatement object will behave similar to a Statement object sending a query text each time it
executes. Otherwise, a client statement prepares the underlying query and obtains required metadata when it is created,
using, for instance, `Connection.prepareStatement(String)` method. The metadata includes such info as an internal
*statement ID*, result set metadata, and query parameters metadata. Once statement is prepared it is possible to
retrieve them using `PreparedStatement.getMetaData()` and `PreparedStatement.getParameterMetaData()` respectively. 

**Note:** It will raise an exception if `getMetaData()` or `getParameterMetaData()` methods are called and current
Tarantool instance does not support the prepared statements. However, `getMetaData()` can be used after the statement is
executed at least once (providing the same behaviour as `PreparedStatement.getResultSet().getMetaData()`). 

But Tarantool imposes a set of rules to be taken into account:

- Tarantool requires explicit preparation and de-allocation for each statement entry. It causes two extra requests, the
first `PREPARE` is sent when a statement is created using `Connection.prepareStatement()` methods family and the second
`DEALLOCATE` *may* be sent after it is released using `PrepareStatement.close()` (it depends on the internal
connection's counter that tract how many shared prepared statements are used per session; see next paragraph to find out
more about the session scope counter). 

- In fact, Tarantool uses the global cache to store prepared statements and share same statements between sessions. But
to manage them without an additional overhead Tarantool counts references of the same query as one per session. This
leads if a JDDC connection produces several similar prepared statements - only one real prepared statement will be
counted by Tarantool. Then, if some of those statements is closed it will remove the corresponding server prepared
statement and breaks down all other statements. It is not ordinary behaviour for users who work with JDBC driver and can
create a lot of identical queries expecting no impaction on each other. To avoid this the driver tracks its own
statement references instantiated by this connection. Each statement producing increases a reference by one and each
statement releasing decreases by one. When connection's reference counter reaches zero only then the connection issues
`DEALLOCATE` query to remove the cached statement.

Let's take a look on the default behaviour in Tarantool:

```lua
s1 = box.prepare("SELECT 1;") -- ref count is 1
s2 = box.prepare("SELECT 1;") -- ref count is still 1 

s1:execute() -- works ok
s2:execute() -- works ok
s1:unprepare() -- count is 0 -> remove the cached statement entry 
s2:execute() -- here we experience an error that statement does not exist anymore
```

slightly different behaviour using `PreparedStatements`:

```java
    PreparedStatement statement1 = connection.prepareStatement("SELECT 1;")); // driver's count is 1, db count is 1
    PreparedStatement statement2 = connection.prepareStatement("SELECT 1;")); // driver's count is 2, db count is 1

    statement1.execute(); // works ok
    statement2.execute(); // works ok

    statement1.close(); // driver's count is 1, db count is 1    

    statement2.execute(); // still works ok
    statement2.close(); // driver's count is 0 -> deallocate server statement -> db count is 0
}
```

- Tarantool invalidates all prepared statements after DDL-query is committed (a statement is associated with the
particular schema version in Tarantool and cannot be used if schema is changed; re-preparation is also not supported by
the server now) and session scoped statements after the current session ends. In this way, already created
`PreparedStatement` objects may return an error if their server prepared statements are invalidated. That repeats the
behaviour that is implemented in Tarantool `box` module:

```lua
s = box.prepare("SELECT * FROM ads WHERE clicks > ?;")
s:execute({3000}); -- works ok
box.execute("DROP TABLE accounts");
s:execute({4000}) -- fails
```

and the same behaviour but for `PreparedStatements`:

```java
try (PreparedStatement statement = connection.prepareStatement("SELECT * FROM ads WHERE clicks > ?;")) {
    statement.setInt(1, 3000);
    statement.execute(); // works ok

    try (Statement dropAccounts = connection.createStatement()) {
        dropAccounts.execute("DROP TABLE accounts");
    }

    statement.setInt(1, 4000);
    statement.execute(); // fails
}
```

References:
1. [Tarantool rfc-2592. SQL: Prepared Statements](https://github.com/tarantool/tarantool/blob/master/doc/rfc/2592-prepared-statement.md)

### Exploring a result columns info

In some circumstances it may be quite helpful to get to know what the columns and their attributes a result set consists
of. For instance, in cases where the SQL statement being executed is unknown until runtime, the result set metadata can
be used to determine which of the getter methods should be used to retrieve the data.

To obtain such info you have to call `ResultSet.getMetaData()`, it returns a `ResultSetMetaData` object describing the
columns of that `ResultSet` object.

An example of how result set metadata can be used to determine the display names of each column in the result set.

```java
try (ResultSet resultSet = statement.executeQuery(sqlString)) {
    ResultSetMetaData metadata = resultSet.getMetaData();
    int columnsSize = metadata.getColumnCount()

    List<String> columnLabels = new List<String>(columnsSize);
    // keep in mind that the result set has an 1-based index
    for (int i = 0, int j = 1; i < columnsSize; i++, j++) {
        colType[i] = metadata.getColumnLabel(j);
    }
    printResultColumns(columnLabels);
}
```

References:
1. [java.sql.ResultSetMetaData](https://docs.oracle.com/javase/8/docs/api/java/sql/ResultSetMetaData.html).

### Processing a result

An example that shows how the `ResultSet` can be obtained:

```java
ResultSet resultSet = statement.executeQuery("SELECT * FROM animal a WHERE a.class = 'mammal'");
```

Next, the obtained `ResultSet` can be used to traverses a set using group of appropriate methods (`next()`,
`previous()`, `first()`, `last()` and others). And columns can be received via a rich family of getter methods.

```java
resultSet.afterLast();
while (resultSet.previous()) {
    processRow(resultSet.getString(1), resultSet.getInt(2));
}
```

The statements support producing `ResultSet` with the characteristics as follows:

There are three result set types such as `ResultSet.TYPE_FORWARD_ONLY`, `ResultSet.TYPE_SCROLL_INSENSITIVE`, and
`ResultSet.TYPE_SCROLL_SENSITIVE`. A forward only result set is not scrollable; its cursor moves only forward using
`next()` method. A scrollable *insensitive* result set has a cursor that can move both forward and backward relative to
the current position as well as move to an absolute position. The result set does not reflect changes made to the
underlying data source while it is open. But a scrollable *sensetive* result set does it. The driver does not support
`ResultSet.TYPE_SCROLL_SENSITIVE` as a result set type that means the obtained results contain that satisfy the query
at the time the query is executed.

Next, there are two concurrency modes `ResultSet.CONCUR_READ_ONLY` and `ResultSet.CONCUR_UPDATABLE`. The only
difference between them whether we use `ResultSet.updateXXX()` family of methods or not. Now driver does not support
`ResultSet.CONCUR_UPDATABLE` and all the result sets released by the driver are immutable and it is impossible to use
*update* methods to modify the data and reflect the changes back to the database.

`ResultSet.HOLD_CURSORS_OVER_COMMIT` and `ResultSet.CLOSE_CURSORS_AT_COMMIT` are options to configure the holdability.
The holdability says should the result set be closed after the transaction is committed (using `Connection.commit()`
method). The transactions are not supported yet and a connection is always in *auto-committed* mode which leads all SQL
statements will be executed and committed as individual transactions. In this way the driver support only
`ResultSet.HOLD_CURSORS_OVER_COMMIT` that is used by default according to the JDBC specification.

All properties listed above can be set during a statement creation using `Conection.createStatement(...)` or
`Connection.prepareStatement(...)` families of methods.

Let's build a new scrollable read-only statement as:

```java
Connection connection = dataSource.getConnection("admin", "p@$$vv0rd");
Statement statement = connection.createStatement(
    ResultSet.TYPE_SCROLL_INSENSITIVE,
    ResultSet.CONCUR_READ_ONLY,
    ResultSet.CLOSE_CURSORS_AT_COMMIT
);
```

A client code can provide an optional hint to the driver how to traverse the obtained cursor inside a result set;
from first-to-last, last-to-first, or using some other unknown strategy to process rows. There are there options that
can be proposed using `Statement.setFetchDirection(int)` and `ResultSet.setFetchDirection(int)` methods. They are
`ResultSet.FETCH_FORWARD`, `ResultSet.FETCH_REVERSE`, `ResultSet.FETCH_FORWARD` hints. The driver always uses
*first-to-last* approach, so `ResultSet.FETCH_FORWARD` is the only supported option now; other hints are ignored by the
driver.

**Note:** Tarantool does not support cursors now. The driver keeps all rows in memory that allows to traverse through
the rows using bi-directional, relative, and absolute movements. The forward only mode does not make sense here a lot
but is supported as a mandatory JDBC feature.

References:
1. [java.sql.ResulSet](https://docs.oracle.com/javase/8/docs/api/java/sql/ResultSet.html).

#### Closing a result set

It is a best practice to use `AutoClosable` interface and the _try-with-resources_ block. It allows to close
unwanted instances and release their resources acquired.

```java
try (
    Statement statement = connection.createStatement();
    ResultSet resultSet = statement.executeQuery("SELECT e.* FROM employee e");
) {
    // process the result
}
```

#### Getting auto-generated values

It's possible to receive back the values being generated by server side and unknown before the execution. Tarantool DB
supports such values to be returned for _auto-increment_ primary keys (keys marked by `AUTOINCREMENT` modifier). When
you execute query you can specify an intention to receive generated keys using a predefined constant
`Statement.RETURN_GENERATED_KEYS` for `Connection.prepareStatement()` or a set of appropriate methods for a `Statement`
object. Next, a call of `Statement.getGeneratedKeys()` returns a single column data set that contains all generated keys
if any. The name of this column is always `GENERATED_KEY`.

Consider a simple example how to get the generated values:

```java
// CREATE TABLE tag (id INT PRIMARY KEY AUTOINCREMENT, tag_name TEXT);
statement.executeUpdate(
    "INSERT INTO tag VALUES (null, 'photo'), (null, 'music')",
    Statement.RETURN_GENERATED_KEYS
);
int rows = statement.getUpdateCount();
if (rows > 0) {
    List<Integer> ids = new ArrayList<>();
    try (ResultSet generatedKeys = statement.getGeneratedKeys()) {
        while (generatedKeys.next()) {
            int newId = generatedKeys.getInt("GENERATED_KEY");
            ids.add(newId);
        }
    }
}
statement.close();
processNewTags(ids);
```

**Note**: The `Statement.getGeneratedKeys()` should be called once. The received `ResultSet` object will be the same
if `getGeneratedKeys()` is invoked multiple times. Once this object is closed it cannot be obtain again using
`getGeneratedKeys()`.

#### Batching updates

The `Statement` and `PreparedStatement` objects can be used to submit batch updates.

For instance, using `Statement` you can send a bundle of heterogeneous statements:

```java
Statement statement = connection.createStatement();
statement.addBatch("INSERT INTO student VALUES (30, 'Joe Jones')");
statement.addBatch("INSERT INTO faculty VALUES (2, 'Faculty of Chemistry')");
statement.addBatch("INSERT INTO student_faculty VALUES (30, 2)");

int[] updateCounts = statement.executeBatch();
```

Using `PreparedStatement` you can reuse bindings of the same statement several times and send a set of the input
parameters as a single unit:

```java
PreparedStatement statement = connection.prepareStatement("INSERT INTO student VALUES (?, ?)");

statement.setInt(1, 30);
statement.setString(2, "Michael Korj");
statement.addBatch();

statement.setInt(1, 40);
statement.setString(2, "Linda Simpson");
statement.addBatch();

int[] updateCounts = statement.executeBatch();
```

The driver uses a pipelining when it performs a batch request. All requests will be sent to a socket in order they were
specified in the batch without synchronous waiting for a result, then the driver will wait for all results. This way the
driver eliminates network latency influence on second and following requests that can give significant performance boost
for large batches, especially when a network latency is high.

**Note**: JDBC spec recommends that *auto-commit* mode should be disabled to prevent the driver from committing a
transaction when a batch request is called. The driver is not support transactions and *auto-commit* is always enabled,
so each statement from the batch is executed in its own transaction. In particular, it causes the driver to continue
processing the remaining commands in a batch if the execution of a command fails.

**Note**: There are some case when an order of batch requests can be corrupted. The first case is that DDL requests are
not transactional in Tarantool. Thus, a batch containing such operations can produce an undefined behaviour:

```java
// second statement can fail with an error that `student` table does not exist
statement.addBatch("CREATE TABLE student (id INT PRIMARY KEY, name VARCHAR(100))");
statement.addBatch("INSERT INTO student VALUES (1, 'Alex Smith')");
```

Moreover, if `vinyl` storage engine is used an execution order of batch statements is also not specified. This behaviour
is incompatible with JDBC spec in the sentence "Batch commands are executed serially (at least logically) in the order
in which they were added to the batch".

### Closing a statement

The best option here is to use statements with _try-with-resources_ construction that allow to release all held
resources just after a statement object is not needed anymore. The closing a statement also closes all result sets
created by the statement including generated keys result set.

There is an optimal structure of a local statement usage

```java
try (Statement statement = connection.createStatement()) {
    // deal with the statement
}
```

### Using escape syntax

JDBC allows to use a special vendor-free syntax called _escape syntax_. Escape syntax is designed to be easily scanned
and parsed by the driver and processed in the vendor-specific way. Implementing this special processing in the driver
layer improves application portability.

Escape processing for a `Statement` object is turned on or off using the method `setEscapeProcessing(bool)`, with the
default being on.

**Note**: The `PreparedStatement.setEscapeProcessing()` method has no effect because its SQL string may have been
precompiled prior to making this call.

`Connection.nativeSQL(String)` provides another way to have escapes processed. It translates the given SQL to a SQL
suitable for the Tarantool database.

JDBC defines escape syntax for the following:

* scalar functions (partially supported)
* date and time literals (not supported)
* outer joins (supported)
* calling stored procedures and functions (not supported)
* escape characters for LIKE clauses (supported)
* returned rows limit (supported)

Let's consider the following case of using escapes:

```java
Statement statement = connection.createStatement();
ResultSet resultSet = statement.executeQuery(
    "SELECT {fn concat(e.name, m.name)} " +
    "FROM {oj employee e LEFT OUTER JOIN employee m ON e.manager_id = m.id} {limit 5}"
);
```

which can be translated to less portable version:

```java
Statement statement = connection.createStatement();
ResultSet resultSet = statement.executeQuery(
    "SELECT (e.name || m.name) " +
    "FROM employee e LEFT OUTER JOIN employee m ON e.manager_id = m.id limit 5"
);
```

**Note**: The driver uses quite simple rules processing escape syntax where the most of the supported expressions
translates directly by omitting escape boundaries `{}`. Thus, the `{fn abs(-5)}}` becomes `abs(-5)` or
`{limit 10 offset 50}` becomes `limit 10 offset 50` and so on. It may lead generating an incorrect SQL statement if you
use bad escape expressions such as `{limit 'one' offset 'zero'}` or `{escape 45}` etc. The escape processing does not
include a full parsing of SQL statements to be sure they are valid (for instance, `SELECT {limit 1} * FROM table`).

#### Scalar functions

The escape syntax to call a scalar function is: `{fn <function_name>(<argument_list>)}`. The driver supports mixing of
other supported escaped functions within `{fn }` block (say, `{fn {fn} + {fn}})`. An attempt to refer to an unsupported
function signature will cause a syntax error.

To find out what the actual set of function is supported by your driver version, you can use `DatabaseMetaData` object
with `getNumericFunctions`, `getStringFunctions`, `getSystemFunctions()`.

Full list of required by JDBC spec can be found in *JDBC 4.2 Specification. Appendix C. Scalar functions*.

The following tables show which functions are supported by the driver.

**Supported numeric scalar functions:**

JDBC escape           | Native                | Comment
--------------------- | --------------------- | ----------------------------------------------------------------
ABS(number)           | ABS(number)           |
PI()                  | 3.141592653589793     | Driver replaces the function by the `java.lang.Math.PI` constant
RAND(seed)            | 0.6005595572679824    | Driver replaces the function to the decimal value `0 <= x < 1` using `Random.nextDouble()`. Seed parameter is ignored
ROUND(number, places) | ROUND(number, places) |

**Supported string scalar functions:**

JDBC escape                                  | Native                                     | Comment
-------------------------------------------- | ------------------------------------------ | -----------------------------------------
CHAR(code)                                   | CHAR(code)                                 |
CHAR_LENGTH(code, CHARACTERS])               | CHAR_LENGTH(code)                          | Last optional parameter is not supported
CHARACTER_LENGTH(code, CHARACTERS])          | CHARACTER_LENGTH(code)                     | Last optional parameter is not supported
CONCAT(string1, string2)                     | (string1 \|\| string2)                     |
LCASE(string)                                | LOWER(string)                              |
LEFT(string, count)                          | SUBSTR(string, 1, count)                   |
LENGTH(string, CHARACTERS)                   | LENGTH(TRIM(TRAILING FROM string))         | Last optional parameters is not supported
LTRIM(string)                                | TRIM(LEADING FROM string)                  |
REPLACE(string1, string2, string3)           | REPLACE(string1, string2, string3)         |
RIGHT(string, count)                         | SUBSTR(string, -(count))                   |
RTRIM(string)                                | TRIM(TRAILING FROM string)                 |
SOUNDEX(string)                              | SOUNDEX(string)                            |
SUBSTRING(string, start, length, CHARACTERS) | SUBSTR(string, start, length)              | Last optional parameters is not supported
UCASE(string)                                | UPPER(string)                              |

**Supported system scalar functions:**

JDBC escape                      | Native                           | Comment
-------------------------------- | -------------------------------- | ----------------------------------------------------------------
DATABASE()                       | 'universe'                       | Tarantool does not support databases. Driver always replaces it to 'universe'
IFNULL(expression1, expression2) | IFNULL(expression1, expression2) |
USER()                           | <string literal>                 | Driver replaces the function to the current user name

Let's take a look at a few examples of using scalar functions:

```java
Statement statement = connection.createStatement();
// SELECT 'DEFAULT'
ResultSet resultSet = statement.executeQuery("SELECT {fn database()}");
```

```java
Statement statement = connection.createStatement();
// SELECT UPPER('usa')
ResultSet resultSet = statement.executeQuery("SELECT {fn ucase('usa')}");
```

```java
Statement statement = connection.createStatement();
// SELECT 2 * 3.141592653589793 * 3.141592653589793 / ABS(RANDOM() - ROUND(3.141592653589793, 4))
ResultSet resultSet = statement.executeQuery(
    "SELECT 2 * {fn pi()} * {fn pi()} / {fn abs({fn rand(252)} - {fn round({fn pi()}, 4)})}"
);
```

#### Outer joins

The escape syntax for an outer join is: `{oj <outer-join>}` where `<outer-join>` has the form:
` table {LEFT|RIGHT|FULL} OUTER JOIN {table | <outer-join>} ON search-condition`

The following SELECT statement uses the escape syntax for an outer join:

```java
Statement statement = connection.createStatement();
// SELECT * FROM employee e LEFT OUTER JOIN department d ON e.dept_id = d.id
ResultSet resultSet = statement.executeQuery(
    "SELECT * FROM {oj employee e LEFT OUTER JOIN department d ON e.dept_id = d.id}"
);
```

Tarantool supports the following JOIN clauses:

* Inner join: `table [NATURAL] [INNER] JOIN {table | joined-table} [<on-or-using>]`.
* Left outer join: `table [NATURAL] LEFT [OUTER] JOIN {table | joined-table} [<on-or-using>]`.
* Cross join: `table CROSS JOIN {table | joined-table}`.

where:
*`<on-or-using> ::= ON search-condition | USING(columns-list)`
* `NATURAL`, `ON` and `USING` are mutually exclusive.

The driver does not perform full parsing of an escaped SQL clause, so it technically allows any JOIN inside `{oj }`
block. However it is recommended to use escape syntax only for outer joins, otherwise a request will not be portable and
so using of escape syntax becomes pointless.

Since Tarantool does not support RIGHT and FULL outer joins, the following subset of clauses is both portable and
supported:

`<outer-join> ::= table LEFT OUTER JOIN {table | <outer-join>} ON search-condition`

#### `LIKE` escape characters

The percent sign `%` and underscore `_` characters are wild card characters in SQL LIKE clauses. To use them as
literal symbols, they can be preceded by a backslash `\`, which is a special escape character in strings. You can
specify which character to use as the escape character by including the following syntax at the end of a LIKE predicate:
`{escape '<escape-character>'}`.

For example, let's compare string values using '|' as an escape character to protect '%' character:

```java
Statement statement = connection.createStatement();
// SELECT * FROM item WHERE description LIKE '|%type' escape '|'
ResultSet resultSet = statement.executeQuery(
    "SELECT * FROM item WHERE description LIKE '|%type' {escape '|'}"
);
```

#### Limiting returned rows escape

The escape syntax for limiting the number of rows returned by a query is: `{limit rows [offset row_offset]}`.

The value given for `rows` indicates the maximum number of rows to be returned from this query. The `row_offset`
indicates the number of rows to skip from the rows returned from the query before beginning to return rows.

The following query will return no more than 10 rows:

```java
Statement statement = connection.createStatement();
// SELECT * FROM student LIMIT 10
ResultSet resultSet = statement.executeQuery("SELECT * FROM table {limit 10}");
```
