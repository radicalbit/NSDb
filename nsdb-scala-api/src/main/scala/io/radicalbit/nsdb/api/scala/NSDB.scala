package io.radicalbit.nsdb.api.scala

import io.radicalbit.nsdb.api.scala.NSDB.DimensionAPI
import io.radicalbit.nsdb.client.rpc.GRPCClient
import io.radicalbit.nsdb.common.JSerializable
import io.radicalbit.nsdb.rpc.common.{Dimension, Bit => GrpcBit}
import io.radicalbit.nsdb.rpc.request._
import io.radicalbit.nsdb.rpc.response.RPCInsertResult

import scala.concurrent.{ExecutionContext, Future}

object NSDB {

  type Metric[T]    = (String, T)
  type DimensionAPI = (String, JSerializable)
  type Field        = (String, JSerializable)

  private val host = "127.0.0.1"

  private val port = 2552

  def connect(host: String = host, port: Int = port, db: String)(
      implicit executionContextExecutor: ExecutionContext): NSDB =
    new NSDB(host = host, port = port, db = db)
}

case class NSDB(host: String, port: Int, db: String)(implicit executionContextExecutor: ExecutionContext) {

  private val client = new GRPCClient(host = host, port = port)

  def namespace(name: String): Namespace = Namespace(name)

  def write(bit: Bit): Future[RPCInsertResult] =
    client.write(
      RPCInsert(
        database = db,
        namespace = bit.namespace,
        metric = bit.metric,
        timestamp = bit.ts getOrElse System.currentTimeMillis,
        value = bit.value match {
          case Some(v: Double) => RPCInsert.Value.DecimalValue(v)
          case Some(v: Long)   => RPCInsert.Value.LongValue(v)
          case unknown         => sys.error(s"The data type ${unknown.getClass.getTypeName} is not supported at the moment.")
        },
        dimensions = bit.dimensions.map {
          case (k, v: java.lang.Double)  => (k, Dimension(Dimension.Value.DecimalValue(v)))
          case (k, v: java.lang.Long)    => (k, Dimension(Dimension.Value.LongValue(v)))
          case (k, v: java.lang.Integer) => (k, Dimension(Dimension.Value.LongValue(v.longValue())))
          case (k, v)                    => (k, Dimension(Dimension.Value.StringValue(v.toString)))
        }.toMap
      ))

  // FIXME: this is not optimized, we should implement a bulk feature
  def write(bs: List[Bit]): Future[List[RPCInsertResult]] =
    Future.sequence(bs.map(x => write(x)))

  def close() = {}
}

case class Namespace(name: String) {

  def bit(bit: String): Bit = Bit(namespace = name, metric = bit)

}

case class Bit(namespace: String,
               metric: String,
               ts: Option[Long] = None,
               private val valueDec: Option[Double] = None,
               private val valueLong: Option[Long] = None,
               dimensions: List[DimensionAPI] = List.empty[DimensionAPI]) {

  def value(v: Long) = copy(valueDec = None, valueLong = Some(v))

  def value(v: Double) = copy(valueDec = Some(v), valueLong = None)

  def value: Option[AnyVal] = valueDec orElse valueLong

  def dimension(dim: DimensionAPI): Bit = copy(dimensions = dimensions :+ dim)

  def timestamp(v: Long): Bit = copy(ts = Some(v))
}
