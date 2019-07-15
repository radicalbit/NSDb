/*
 * Copyright 2018 Radicalbit S.r.l.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.radicalbit.nsdb.api.scala

import io.radicalbit.nsdb.api.scala.NSDB._
import io.radicalbit.nsdb.client.rpc.GRPCClient
import io.radicalbit.nsdb.rpc.common.{Dimension, Tag}
import io.radicalbit.nsdb.rpc.health.HealthCheckResponse
import io.radicalbit.nsdb.rpc.request.RPCInsert
import io.radicalbit.nsdb.rpc.requestSQL.SQLRequestStatement
import io.radicalbit.nsdb.rpc.response.RPCInsertResult
import io.radicalbit.nsdb.rpc.responseSQL.SQLStatementResponse

import scala.collection.mutable.ListBuffer
import scala.concurrent.{ExecutionContext, Future}

/**
  * Provides method to create a connection to an instance of Nsdb
  */
object NSDB {

  type DimensionAPI = (String, Dimension)

  type TagAPI = (String, Tag)

  /**
    * Connect to a Nsdb instance.
    * @param host instance host.
    * @param port instance port.
    * @param executionContextExecutor implicit execution context.
    * @return a Future of a nsdb connection.
    */
  def connect(host: String, port: Int)(implicit executionContextExecutor: ExecutionContext): Future[NSDB] = {
    val connection = new NSDB(host = host, port = port)
    connection.check.map(_ => connection)
  }

  implicit def NSDBToNSDBMetricInfo(str: NSDB): NSDBMetricInfo = new NSDBMetricInfo(str)
  implicit def NSDBToNSDBDescribeInfo(str: NSDB): NSDBDescribe = new NSDBDescribe(str)
}

/**
  * Nsdb connection class.
  * Provides methods to define a bit to be inserted and a query to be executed leveraging a builder pattern.
  * {{{
  *   val nsdb = Await.result(NSDB.connect(host = "127.0.0.1", port = 7817)(ExecutionContext.global), 10.seconds)

      val series = nsdb
        .db("root")
        .namespace("registry")
        .metric("people")
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
case class NSDB(host: String, port: Int)(implicit executionContextExecutor: ExecutionContext) {

  /**
    * Inner Grpc client.
    */
  protected[scala] val client = new GRPCClient(host = host, port = port)

  /**
    * Defines the db used to build the bit or the query.
    * @param name the db name
    */
  def db(name: String): Db = Db(name)

  /**
    * Checks if a connection is healthy.
    */
  def check: Future[HealthCheckResponse] = client.checkConnection()

  /**
    * Writes a bit into Nsdb using the current openend connection.
    * @param bit the bit to be inserted.
    * @return a Future containing the result of the operation. See [[RPCInsertResult]].
    */
  def write(bit: Bit): Future[RPCInsertResult] =
    client.write(
      RPCInsert(
        database = bit.db,
        namespace = bit.namespace,
        metric = bit.metric,
        timestamp = bit.timestamp getOrElse System.currentTimeMillis,
        value = bit.value,
        dimensions = bit.dimensions.toMap,
        tags = bit.tags.toMap
      ))

  /**
    * Writes a list of bits into NSdb using the current openend connection.
    * @param bs the list of bits to be inserted.
    * @return a Future containing the result of the operation. See [[RPCInsertResult]].
    */
  def write(bs: List[Bit]): Future[List[RPCInsertResult]] =
    Future.sequence(bs.map(x => write(x)))

  /**
    * Executes a [[SQLStatement]] using the current openend connection.
    * @param sqlStatement the [[SQLStatement]] to be executed.
    * @return a Future of the result of the operation. See [[SQLStatementResponse]].
    */
  def execute(sqlStatement: SQLStatement): Future[SQLStatementResponse] = {
    val sqlStatementRequest =
      SQLRequestStatement(db = sqlStatement.db,
                          namespace = sqlStatement.namespace,
                          statement = sqlStatement.sQLStatement)
    client.executeSQLStatement(sqlStatementRequest)
  }

  /**
    * Closes the connection. Here any allocated resources must be released.
    */
  def close(): Unit = {}
}

/**
  * Auxiliary case class used to define the Db for the builder.
  * @param name db name.
  */
case class Db(name: String) {

  /**
    * Defines the namespace for the builder.
    * @param namespace namespace name.
    * @return auxiliary namespace instance.
    */
  def namespace(namespace: String): Namespace = Namespace(name, namespace)

}

/**
  * Auxiliary case class used to define the Namespace for the Bit builder.
  * @param name the namespace name.
  */
case class Namespace(db: String, name: String) {

  /**
    * Builds the metric to be inserted.
    *
    * @param metric metric name.
    * @return a bit with empty dimensions and value.
    */
  def metric(metric: String): Bit = Bit(db = db, namespace = name, metric = metric)

  /**
    * Builds the query to be executed.
    * @param queryString raw query.
    * @return auxiliary [[SQLStatement]] case class.
    */
  def query(queryString: String) = SQLStatement(db = db, namespace = name, sQLStatement = queryString)

}

case class SQLStatement(db: String, namespace: String, sQLStatement: String)

/**
  * Auxiliary case class useful to define a bit to be inserted.
  * @param db the db.
  * @param namespace the namespace.
  * @param metric the metric.
  * @param timestamp bit timestamp.
  * @param value bit value.
  * @param dimensions bit dimensions list.
  */
case class Bit protected (db: String,
                          namespace: String,
                          metric: String,
                          timestamp: Option[Long] = None,
                          value: RPCInsert.Value = RPCInsert.Value.Empty,
                          dimensions: ListBuffer[DimensionAPI] = ListBuffer.empty[DimensionAPI],
                          tags: ListBuffer[TagAPI] = ListBuffer.empty[TagAPI]) {

  /**
    * Builds [[MetricInfo]] from an existing bit providing a shard interval
    * @param interval the shard interval expressed in the Duration pattern (2d, 1h ecc.)
    * @return the resulting [[MetricInfo]]
    */
  def shardInterval(interval: String): MetricInfo = MetricInfo(db, namespace, metric, Some(interval), None)

  /**
    * Builds [[MetricInfo]] from an existing bit providing a retention
    * @param retention the metric duration expressed in the Duration pattern (2d, 1h ecc.)
    * @return the resulting [[MetricInfo]]
    */
  def retention(retention: String): MetricInfo = MetricInfo(db, namespace, metric, None, Some(retention))

  /**
    * Adds a Long value to the bit.
    * @param v the Long value.
    * @return a new instance with `v` as the value.
    */
  def value(v: Long): Bit = copy(value = RPCInsert.Value.LongValue(v))

  /**
    * Adds a Int value to the bit.
    * @param v the Int value.
    * @return a new instance with `v` as the value.
    */
  def value(v: Int): Bit = copy(value = RPCInsert.Value.LongValue(v))

  /**
    * Adds a Double value to the bit.
    * @param v the Double value.
    * @return a new instance with `v` as the value.
    */
  def value(v: Double): Bit = copy(value = RPCInsert.Value.DecimalValue(v))

  /**
    * Adds a [[java.math.BigDecimal]] value to the bit.
    * @param v the BigDecimal value.
    * @return a new instance with `v` as the value.
    */
  def value(v: java.math.BigDecimal): Bit = if (v.scale() > 0) value(v.doubleValue()) else value(v.longValue())

  /**
    * Adds a Long dimension to the bit.
    * @param k the dimension name.
    * @param d the Long dimension value.
    * @return a new instance with `(k -> d)` as a dimension.
    */
  def dimension(k: String, d: Long): Bit = {
    dimensions += ((k, Dimension(Dimension.Value.LongValue(d))))
    this
  }

  /**
    * Adds a Int dimension to the bit.
    * @param k the dimension name.
    * @param d the Int dimension value.
    * @return a new instance with `(k -> d)` as a dimension.
    */
  def dimension(k: String, d: Int): Bit = {
    dimensions += ((k, Dimension(Dimension.Value.LongValue(d.longValue()))))
    this
  }

  /**
    * Adds a Double dimension to the bit.
    * @param k the dimension name.
    * @param d the Double dimension value.
    * @return a new instance with `(k -> d)` as a dimension.
    */
  def dimension(k: String, d: Double): Bit = {
    dimensions += ((k, Dimension(Dimension.Value.DecimalValue(d))))
    this
  }

  /**
    * Adds a String dimension to the bit.
    * @param k the dimension name.
    * @param d the String dimension value.
    * @return a new instance with `(k -> d)` as a dimension.
    */
  def dimension(k: String, d: String): Bit = {
    dimensions += ((k, Dimension(Dimension.Value.StringValue(d))))
    this
  }

  /**
    * Adds a [[java.math.BigDecimal]] dimension to the bit.
    * @param k the dimension name.
    * @param d the [[java.math.BigDecimal]] dimension value.
    * @return a new instance with `(k -> d)` as a dimension.
    */
  def dimension(k: String, d: java.math.BigDecimal): Bit =
    if (d.scale() > 0) dimension(k, d.doubleValue()) else dimension(k, d.longValue())

  /**
    * Adds a Long tag to the bit.
    * @param k the tag name.
    * @param d the Long tag value.
    * @return a new instance with `(k -> d)` as a tag.
    */
  def tag(k: String, d: Long): Bit = {
    tags += ((k, Tag(Tag.Value.LongValue(d))))
    this
  }

  /**
    * Adds a Int tag to the bit.
    * @param k the tag name.
    * @param d the Int tag value.
    * @return a new instance with `(k -> d)` as a tag.
    */
  def tag(k: String, d: Int): Bit = {
    tags += ((k, Tag(Tag.Value.LongValue(d.longValue()))))
    this
  }

  /**
    * Adds a Double tag to the bit.
    * @param k the tag name.
    * @param d the Double tag value.
    * @return a new instance with `(k -> d)` as a tag.
    */
  def tag(k: String, d: Double): Bit = {
    tags += ((k, Tag(Tag.Value.DecimalValue(d))))
    this
  }

  /**
    * Adds a String tag to the bit.
    * @param k the tag name.
    * @param d the String tag value.
    * @return a new instance with `(k -> d)` as a tag.
    */
  def tag(k: String, d: String): Bit = {
    tags += ((k, Tag(Tag.Value.StringValue(d))))
    this
  }

  /**
    * Adds a [[java.math.BigDecimal]] tag to the bit.
    * @param k the tag name.
    * @param d the [[java.math.BigDecimal]] tag value.
    * @return a new instance with `(k -> d)` as a tag.
    */
  def tag(k: String, d: java.math.BigDecimal): Bit =
    if (d.scale() > 0) tag(k, d.doubleValue()) else tag(k, d.longValue())

  @deprecated(
    "It's not fully type safe and it's not possible to make it due to our best friend Jvm type erasure. It's better to be removed in order to prevent a non correct usage of the apis",
    "0.7.0"
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
    * Adds a Long timestamp to the bit.
    * @param v the timestamp.
    * @return a new instance with `v` as a timestamp.
    */
  def timestamp(v: Long): Bit = copy(timestamp = Some(v))
}
