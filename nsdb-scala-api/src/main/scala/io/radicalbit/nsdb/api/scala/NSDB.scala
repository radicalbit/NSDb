package io.radicalbit.nsdb.api.scala

import io.radicalbit.nsdb.api.scala.NSDB.DimensionAPI
import io.radicalbit.nsdb.client.rpc.GRPCClient
import io.radicalbit.nsdb.rpc.common.Dimension
import io.radicalbit.nsdb.rpc.health.HealthCheckResponse
import io.radicalbit.nsdb.rpc.request._
import io.radicalbit.nsdb.rpc.requestSQL.SQLRequestStatement
import io.radicalbit.nsdb.rpc.response.RPCInsertResult
import io.radicalbit.nsdb.rpc.responseSQL.SQLStatementResponse

import scala.concurrent.{ExecutionContext, Future}

/**
  * Provides method to create a connection to an instance of Nsdb
  */
object NSDB {

  type DimensionAPI = (String, Dimension)

  /**
    * connect to a Nsdb instance
    * @param host instance host
    * @param port instance port
    * @param executionContextExecutor implicit execution context
    * @return a Future of a nsdb connection
    */
  def connect(host: String, port: Int)(implicit executionContextExecutor: ExecutionContext): Future[NSDB] = {
    val connection = new NSDB(host = host, port = port)
    connection.check.map(_ => connection)
  }
}

/**
  * Nsdb connection class.
  * Provides methods to define a bit to be inserted and a query to be executed leveraging a builder pattern.
  * {{{
  *   val nsdb = Await.result(NSDB.connect(host = "127.0.0.1", port = 7817)(ExecutionContext.global), 10.seconds)

      val series = nsdb
        .db("root")
        .namespace("registry")
        .bit("people")
        .value(Some(new java.math.BigDecimal("13")))
        .dimension("city", "Mouseton")

      nsdb.write(series)

      val query = nsdb
      .db("root")
      .namespace("registry")
      .query("select * from people limit 1")

      val readRes: Future[SQLStatementResponse] = nsdb.execute(query)
  * }}}
  * @param host Nsdb host
  * @param port Nsdb port
  * @param executionContextExecutor implicit execution context to handle asynchronous methods
  */
//TODO see if it make sense to rename this into something like NsdbConnection
case class NSDB(host: String, port: Int)(implicit executionContextExecutor: ExecutionContext) {

  // the inner Grpc client
  private val client = new GRPCClient(host = host, port = port)

  /**
    * defines the db used to build the bit or the query
    * @param name the db name
    * @return
    */
  def db(name: String): Db = Db(name)

  /**
    * check if a connection is healthy
    */
  def check: Future[HealthCheckResponse] = client.checkConnection()

  /**
    * write a bit into Nsdb using the current openend connection
    * @param bit the bit to be inserted
    * @return a Future containing the result of the operation. See [[RPCInsertResult]]
    */
  def write(bit: Bit): Future[RPCInsertResult] =
    client.write(
      RPCInsert(
        database = bit.db,
        namespace = bit.namespace,
        metric = bit.metric,
        timestamp = bit.ts getOrElse System.currentTimeMillis,
        value = bit.value,
        dimensions = bit.dimensions.toMap
      ))

  /**
    * write a list of bits into NSdb using the current openend connection
    * @param bs the list of bits to be inserted
    * @return a Future containing the result of the operation. See [[RPCInsertResult]]
    */
  // FIXME: should we implement a bulk feature ?
  def write(bs: List[Bit]): Future[List[RPCInsertResult]] =
    Future.sequence(bs.map(x => write(x)))

  /**
    * execute a [[SQLStatement]] using the current openend connection
    * @param sqlStatement the [[SQLStatement]] to be executed
    * @return a Future of the result of the operation. See [[SQLStatementResponse]]
    */
  def execute(sqlStatement: SQLStatement): Future[SQLStatementResponse] = {
    val sqlStatementRequest =
      SQLRequestStatement(db = sqlStatement.db,
                          namespace = sqlStatement.namespace,
                          statement = sqlStatement.sQLStatement)
    client.executeSQLStatement(sqlStatementRequest)
  }

  /**
    * close the connection. Here any allocated resources must be released
    */
  def close(): Unit = {}
}

/**
  * Auxiliary case class used to define the Db for the builder
  * @param name the db name
  */
case class Db(name: String) {

  /**
    * define the namespace for the builder
    * @param namespace namespace name
    * @return auxiliary namespace instance
    */
  def namespace(namespace: String): Namespace = Namespace(name, namespace)

}

/**
  * Auxiliary case class used to define the Namespace for the Bit builder
  * @param name the namespace name
  */
case class Namespace(db: String, name: String) {

  /**
    * builds the bit to be inserted
    * @param bit metric name
    * @return a bit with empty dimensions and value
    */
  def bit(bit: String): Bit = Bit(db = db, namespace = name, metric = bit)

  /**
    * builds the query to be executed
    * @param queryString raw query
    * @return auxiliary [[SQLStatement]] case class
    */
  def query(queryString: String) = SQLStatement(db = db, namespace = name, sQLStatement = queryString)

}

case class SQLStatement(db: String, namespace: String, sQLStatement: String)

/**
  * auxiliary case class useful to define a bit to be inserted
  * @param db the db
  * @param namespace the namespace
  * @param metric the metric
  * @param ts bit timestamp
  * @param value bit value
  * @param dimensions bit dimsneions list
  */
case class Bit protected (db: String,
                          namespace: String,
                          metric: String,
                          ts: Option[Long] = None,
                          value: RPCInsert.Value = RPCInsert.Value.Empty,
                          dimensions: List[DimensionAPI] = List.empty[DimensionAPI]) {

  /**
    * adds a Long value to the bit
    * @param v the Long value
    * @return a new instance with `v` as the value
    */
  def value(v: Long): Bit = copy(value = RPCInsert.Value.LongValue(v))

  /**
    * adds a Int value to the bit
    * @param v the Int value
    * @return a new instance with `v` as the value
    */
  def value(v: Int): Bit = copy(value = RPCInsert.Value.LongValue(v))

  /**
    * adds a Double value to the bit
    * @param v the Double value
    * @return a new instance with `v` as the value
    */
  def value(v: Double): Bit = copy(value = RPCInsert.Value.DecimalValue(v))

  /**
    * adds a [[java.math.BigDecimal]] value to the bit
    * @param v the BigDecimal value
    * @return a new instance with `v` as the value
    */
  def value(v: java.math.BigDecimal): Bit = if (v.scale() > 0) value(v.doubleValue()) else value(v.longValue())

  @deprecated("It does make sense. Value must be always defined in a Bit", "0.1.4")
  def value[T](v: Option[T]): Bit = v match {
    case Some(v: Long)                 => value(v)
    case Some(v: Int)                  => value(v)
    case Some(v: Double)               => value(v)
    case Some(v: java.math.BigDecimal) => value(v)
    case _                             => this
  }

  /**
    * adds a Long dimension to the bit
    * @param k the dimension name
    * @param d the Long dimension value
    * @return a new instance with `(k -> d)` as a dimension
    */
  def dimension(k: String, d: Long): Bit =
    copy(dimensions = dimensions :+ (k, Dimension(Dimension.Value.LongValue(d))))

  /**
    * adds a Int dimension to the bit
    * @param k the dimension name
    * @param d the Int dimension value
    * @return a new instance with `(k -> d)` as a dimension
    */
  def dimension(k: String, d: Int): Bit =
    copy(dimensions = dimensions :+ (k, Dimension(Dimension.Value.LongValue(d.longValue()))))

  /**
    * adds a Double dimension to the bit
    * @param k the dimension name
    * @param d the Double dimension value
    * @return a new instance with `(k -> d)` as a dimension
    */
  def dimension(k: String, d: Double): Bit =
    copy(dimensions = dimensions :+ (k, Dimension(Dimension.Value.DecimalValue(d))))

  /**
    * adds a String dimension to the bit
    * @param k the dimension name
    * @param d the String dimension value
    * @return a new instance with `(k -> d)` as a dimension
    */
  def dimension(k: String, d: String): Bit =
    copy(dimensions = dimensions :+ (k, Dimension(Dimension.Value.StringValue(d))))

  /**
    * adds a [[java.math.BigDecimal]] dimension to the bit
    * @param k the dimension name
    * @param d the [[java.math.BigDecimal]] dimension value
    * @return a new instance with `(k -> d)` as a dimension
    */
  def dimension(k: String, d: java.math.BigDecimal): Bit =
    if (d.scale() > 0) dimension(k, d.doubleValue()) else dimension(k, d.longValue())

  @deprecated(
    "It's not fully type safe and it's not possible to make it due to our best friend Jvm type erasure. It's better to be removed in order to prevent a non correct usage of the apis",
    "0.2.0"
  )
  def dimension[T](k: String, d: Option[T]): Bit =
    d match {
      case Some(v: java.math.BigDecimal) => dimension(k, v)
      case Some(v: Long)                 => dimension(k, v)
      case Some(v: Int)                  => dimension(k, v)
      case Some(v: Double)               => dimension(k, v)
      case Some(v: String)               => dimension(k, v)
      case _                             => this
    }

  /**
    * adds a Long timestamp to the bit
    * @param v the timestamp
    * @return a new instance with `v` as a timestamp
    */
  def timestamp(v: Long): Bit = copy(ts = Some(v))
}
