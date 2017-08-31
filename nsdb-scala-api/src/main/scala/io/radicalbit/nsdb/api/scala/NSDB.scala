package io.radicalbit.nsdb.api.scala

import io.radicalbit.nsdb.api.scala.NSDB.Dimension
import io.radicalbit.nsdb.client.rpc.GRPCClient
import io.radicalbit.nsdb.common.JSerializable
import io.radicalbit.nsdb.rpc.request.RPCInsert
import io.radicalbit.nsdb.rpc.request.RPCInsert.Value.{DecimalValue, LongValue}
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

  private val client = new GRPCClient(host = host, port = port)

  def namespace(name: String): Namespace = Namespace(name)

  def write(bit: Bit): Future[RPCInsertResult] =
    client.write(
      RPCInsert(
        namespace = bit.namespace,
        metric = bit.metric,
        timestamp = bit.ts getOrElse (System.currentTimeMillis),
        value = bit.value match {
          case Some(v: Double) => DecimalValue(v)
          case Some(v: Long)   => LongValue(v)
          case _               => sys.error("boom")
        }
      ))

  // FIXME: this is not optimized, we should implement a bulk feature
  def write(bs: List[Bit]): Future[List[RPCInsertResult]] =
    Future.sequence(bs.map(x => write(x)))

  def close() = {}
}

case class Namespace(name: String) {

  def metric(metric: String): Metric = Metric(namespace = name, metric = metric)

}

case class Metric(namespace: String, metric: String) {

  def bit: Bit = Bit(namespace = namespace, metric = metric)

}

case class Bit(namespace: String,
               metric: String,
               ts: Option[Long] = None,
               private val valueDec: Option[Double] = None,
               private val valueLong: Option[Long] = None,
               dimensions: List[Dimension] = List.empty[Dimension]) {

  def value(v: Long) = copy(valueDec = None, valueLong = Some(v))

  def value(v: Double) = copy(valueDec = Some(v), valueLong = None)

  def value: Option[AnyVal] = valueDec orElse valueLong

  def dimension(dim: Dimension): Bit = copy(dimensions = dimensions :+ dim)

  def timestamp(v: Long): Bit = copy(ts = Some(v))
}
