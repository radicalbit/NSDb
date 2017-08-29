package io.radicalbit.nsdb.api.scala

import io.radicalbit.nsdb.api.scala.NSDB.{Dimension, Field, Metric}
import io.radicalbit.nsdb.client.rpc.GRPCClient
import io.radicalbit.nsdb.common.JSerializable
import io.radicalbit.nsdb.rpc.request.RPCInsert
import io.radicalbit.nsdb.rpc.response.RPCInsertResult

import scala.concurrent.{ExecutionContext, Future}

object NSDB {

  type Metric[T] = (String, T)
  type Dimension = (String, JSerializable)
  type Field     = (String, JSerializable)

  private val host = "127.0.0.1"

  private val port = 2552

  def connect(host: String = host, port: Int = port)(implicit executionContextExecutor: ExecutionContext): NSDB =
    new NSDB(host = host, port = port)
}

case class NSDB(host: String, port: Int)(implicit executionContextExecutor: ExecutionContext) {

//  private implicit val client = new Client(host = host, port = port)
  private val client = new GRPCClient(host = host, port = port)

  def namespace(name: String): Namespace = Namespace(name)

  def write[T](bit: Bit[T]): Future[RPCInsertResult] =
    client.write(
      RPCInsert(namespace = bit.namespace,
                metric = bit.series,
                timestamp = bit.ts getOrElse (System.currentTimeMillis)))

//  def write[T](bit: Bit[T]): Future[SQLStatementExecuted] =
//    client.executeSqlStatement(
//      InsertSQLStatement(namespace = bit.namespace,
//                         metric = bit.series,
//                         timestamp = bit.ts,
//                         dimensions = ListAssignment(bit.dimensions.toMap),
//                         fields = ListAssignment(bit.fields.toMap)))

  // FIXME: this is not optimized, we should implement a bulk feature
  def write[T](bs: List[Bit[T]]): Future[List[RPCInsertResult]] =
    Future.sequence(bs.map(x => write(x)))

  def close() = {}
}

case class Namespace(name: String) {

  def series[T](series: String): Series[T] = Series[T](namespace = name, series = series)

}

case class Series[T](namespace: String, series: String) {

  def bit: Bit[T] = Bit(namespace = namespace, series = series)

}

case class Bit[T](namespace: String,
                  series: String,
                  metric: Option[Metric[T]] = None,
                  dimensions: List[Dimension] = List.empty[Dimension],
                  fields: List[Field] = List.empty[Field],
                  ts: Option[Long] = None) {

  def metric(key: String, value: T): Bit[T] = copy(metric = Some((key, value)))

  def dimension(dim: Dimension): Bit[T] = copy(dimensions = dimensions :+ dim)

  def field(field: Field): Bit[T] = copy(fields = fields :+ field)

  def timestamp(v: Long): Bit[T] = copy(ts = Some(v))
}
