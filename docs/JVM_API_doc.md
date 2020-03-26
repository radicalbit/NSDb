# Integration API
NSDb provides a set of APIs written both in Scala and Java to allow third-party systems integration. An example of usage of these APIs is the implementation of the Flink Sink operator.


Despite actual APIs implementation is limited to Java and Scala languages, the same functionalities can be easily implemented in others languages based on the agnostic protocol integration APIs are designed on top of. In fact, they are based on [gRPC](https://grpc.io/) standard that works across multiple languages and platforms.

The main difference between Java and Scala APIs is the way asynchronous calls are handled. In Java, `CompletableFuture` is used, while async results in Scala are wrapped into Scala native `Future`.

## Adding the library to your project

The library artifact is publicly available in a dedicated maven repository, available at https://tools.radicalbit.io/artifactory/public-release/io/radicalbit/nsdb/.

### Release versions

####Maven dependencies:

```
<project>
...
    <repositories>
        <repository>
        <id>NSDb</id>
        <name>NSDb Repo</name>
        <url>https://tools.radicalbit.io/artifactory/public-release/</url>
        </repository>
    </repositories>
...
    <dependency>
        groupId>io.radicalbit.nsdb</groupId>
        <artifactId>nsdb-java-api</artifactId>
        <version>1.0.0</version>
    </dependency>
...
    <dependency>
        groupId>io.radicalbit.nsdb</groupId>
        <artifactId>nsdb-scala-api_2.12</artifactId>
        <version>1.0.0</version>
    </dependency>
...
</<project>
```

####SBT dependencies:

```
resolvers += "NSDb Public Releases" at "https://tools.radicalbit.io/artifactory/public-release/"

libraryDependencies += "io.radicalbit.nsdb" %% "nsdb-scala-api" % "1.0.0"

libraryDependencies += "io.radicalbit.nsdb" % "nsdb-java-api" % "1.0.0"
```

# Java API

NSDb implements utility classes to perform writes and to execute queries using **Java language**.
In both cases, communication to NSDb cluster is handled using a gRPC Client instantiated in ` io.radicalbit.nsdb.api.java.NSDB` connection class.

## Init API
Before writing into a metric, NSDb provides an api that makes possible to set some metric parameter in order to optimize write or read performance.
The Init api allows the user to set the following custom parameters for a metric

- shard interval: the time interval that determines the shard duration.
- retention: the time interval in which data are kept into the indices.

If this api is not called, the default shard interval and an infinite retention will be used for the metric.
The shard interval and the retention are expressed using the [Java Duration](https://docs.oracle.com/javase/8/docs/api/java/time/Duration.html) convention (e.g. 23 seconds, 23 s, 24h etc).
This operation is allowed only before the first bit is written. Otherwise an error message will be returned  

**Example**
```java
public class NSDBInitMetric {

    public static void main(String[] args) throws Exception {
        NSDB nsdb = NSDB.connect("127.0.0.1", 7817).get();

        NSDB.BitInfo metricInfo = nsdb.db("root")
                .namespace("registry")
                .bit("people")
                .shardInterval("2d")
                .retention("2d");


        InitMetricResult result = nsdb.initMetric(metricInfo).get();
        System.out.println("IsSuccessful = " + result.isCompletedSuccessfully());
        System.out.println("errors = " + result.getErrorMsg());
    }
}
```
Results
```
IsSuccessful = true
errors = ""
```

## Write API
The `NSDB` class  exposes a `write` method performing ` io.radicalbit.nsdb.api.java.NSDB.Bit` insert into the specified metric.
The record to be inserted must of class `Bit`. Bit's parameters are defined using build pattern.
Insert operation returns `io.radicalbit.nsdb.api.java.InsertResult` wrapped into a java `CompletableFuture`. `InsertResult` contains a `Boolean` describing request's success or failure and in case of failure the list of errors.

**Example**

```java
public class NSDBWrite {
    public static void main(String[] args) throws Exception {
        NSDB nsdb = NSDB.connect("127.0.0.1", 7817).get();

        NSDB.Bit record = nsdb.db("root")
                .namespace("registry")
                .bit("people")
                .value(new java.math.BigDecimal("13"))
                .dimension("city", "Mouseton")
                .dimension("gender", "M")
                .dimension("double", 12.5)
                .dimension("bigDecimalLong", new java.math.BigDecimal("12"))
                .dimension("bigDecimalDouble", new java.math.BigDecimal("12.5"));

        InsertResult result = nsdb.write(record).get();
        System.out.println("IsSuccessful = " + result.isCompletedSuccessfully());
        System.out.println("errors = " + result.getErrors());
    }
}
```
Results:

```
IsSuccessful = true
errors = ""
```
## Read API
As mentioned above Read API , as Write API, makes use of the same component `NSDB` but calling the `executeStatement` method. `NSDB.executeStatement` accepts a `NSDB.SQLStatement` parameter describing:

- database to run the select statements on
- the underlying namespace
- the query string statement

Similarly to Write API query result is wrapped into a java `CompletableFuture` containg a `io.radicalbit.nsdb.api.java.QueryResult`. `QueryResults` defines a parameter `records` in which query results are collected as a `List<Bit>`.
In case of failure `record` property contains an empty list and `isCompletedSuccessfully` method return false. Failure reason can be viewed using `getReason()` method.

```java
public class NSDBRead {
    public static void main(String[] args) throws Exception {
        NSDB nsdb = NSDB.connect("127.0.0.1", 7817).get();

        NSDB.SQLStatement statement = nsdb.db("root").namespace("registry").query("select * from people limit 1");

        QueryResult result = nsdb.executeStatement(statement).get();

        if (result.isCompletedSuccessfully()) {
            System.out.println("db : " + result.getDb());
            System.out.println("namespace : " + result.getNamespace());
            System.out.println("metric : " + result.getMetric());
            System.out.println("bits : " + result.getRecords());
        } else {
            System.out.println("reason : " + result.getReason());
        }
    }
}
```
Results:
```
db : root
namespace : registry
metric : people
bits : [
timestamp: 1522328164110
longValue: 13
dimensions {
  key: "city"
  value {
    stringValue: "Mouseton"
  }
}
dimensions {
  key: "bigDecimalLong"
  value {
    longValue: 12
  }
}
dimensions {
  key: "double"
  value {
    decimalValue: 12.5
  }
}
dimensions {
  key: "bigDecimalDouble"
  value {
    decimalValue: 12.5
  }
}
dimensions {
  key: "gender"
  value {
    stringValue: "M"
  }
}
]

```

# Scala API
The same capabilities exposed by NSDb's Java API are implemented in Scala API too.
The `io.radicalbit.nsdb.api.scala.NSDB`class provides a method to create a connection to an instance of NSDb. Connection to gRPC NSDb's endpoint is instanciated ayncronously using `connect` methods that require `host` and `port` parameters.

# Init API
As well as for the Java apis, it is possible to set the shard interval for a metric, before the first bit is written.

```scala
object NSDBInitRead extends App {

  val nsdb = Await.result(NSDB.connect(host = "127.0.0.1", port = 7817)(ExecutionContext.global), 10.seconds)

  val init = nsdb
    .db("root")
    .namespace("registry")
    .bit("people")
    .shardInterval("2d")

  val readRes: Future[InitMetricResponse] = nsdb.init(init)

  println(Await.result(readRes, 10.seconds))
}
```

Results:

```
completedSuccessfully: true
errors: ""
```

## Write API
Scala Write API provides `NSDB.write` method used to define an `io.radicalbit.nsdb.api.scala.Bit` to be inserted leveraging a builder pattern.
Asynchronous  response is wrapped into a Scala `Future`.
Response of class `io.radicalbit.nsdb.rpc.response.RPCInsertResult` contains a feedback on request result in `isCompletedSuccessfully` field and in case of failure a string representing occurred errors.

```scala
object NSDBMainWrite extends App {
    val nsdb = Await.result(NSDB.connect(host = "127.0.0.1", port = 7817)(ExecutionContext.global), 10.seconds)

      val series = nsdb
        .db("root")
        .namespace("registry")
        .bit("people")
        .value(new java.math.BigDecimal("13"))
        .dimension("city", "Mouseton")
        .dimension("double", 12.5)

     val res: Future[RPCInsertResult] =  nsdb.write(series)
     println(Await.result(res, 10.seconds))
}
```

Results:

```
completedSuccessfully: true
errors: ""
```

## Read API
Read API allows to run query statements on NSDb, returning an `io.radicalbit.nsdb.rpc.responseSQL.SQLStatementResponse` containing selected rows.

> NOTE: SQLStatementResponse is a case class generated using protobuf schema definition.


```scala
object NSDBMainRead extends App {
    val nsdb = Await.result(NSDB.connect(host = "127.0.0.1", port = 7817)(ExecutionContext.global), 10.seconds)

  val statement = nsdb
    .db("root")
    .namespace("registry")
    .query("select * from people limit 1")

  val res: Future[SQLStatementResponse] = nsdb.execute(statement)

  println(Await.result(res, 10.seconds))
}
```

Results:

```
db: "root"
namespace: "registry"
metric: "people"
completedSuccessfully: true
reason: ""
message: ""
records {
  timestamp: 1522329103520
  longValue: 13
  dimensions {
    key: "city"
    value {
      stringValue: "Mouseton"
    }
  }
  dimensions {
    key: "bigDecimalLong"
    value {
      longValue: 12
    }
  }
  dimensions {
    key: "Someimportant"
    value {
      longValue: 2
    }
  }
  dimensions {
    key: "OptionBigDecimal"
    value {
      decimalValue: 15.5
    }
  }
  dimensions {
    key: "bigDecimalDouble"
    value {
      decimalValue: 12.5
    }
  }
  dimensions {
    key: "gender"
    value {
      stringValue: "M"
    }
  }
}
```
