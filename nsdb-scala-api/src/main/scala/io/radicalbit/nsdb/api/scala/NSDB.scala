package io.radicalbit.nsdb.api.scala

import io.radicalbit.nsdb.api.scala.NSDB.DimensionAPI
import io.radicalbit.nsdb.client.rpc.GRPCClient
import io.radicalbit.nsdb.common.JSerializable
import io.radicalbit.nsdb.rpc.common.Dimension
import io.radicalbit.nsdb.rpc.health.HealthCheckResponse
import io.radicalbit.nsdb.rpc.request._
import io.radicalbit.nsdb.rpc.requestSQL.SQLRequestStatement
import io.radicalbit.nsdb.rpc.response.RPCInsertResult
import io.radicalbit.nsdb.rpc.responseSQL.SQLStatementResponse

import scala.concurrent.{ExecutionContext, Future}

object NSDB {

  type Metric[T]    = (String, T)
  type DimensionAPI = (String, Any)

  def connect(host: String, port: Int)(implicit executionContextExecutor: ExecutionContext): Future[NSDB] = {
    val connection = new NSDB(host = host, port = port)
    connection.check.map(_ => connection)
  }
}

case class NSDB(host: String, port: Int)(implicit executionContextExecutor: ExecutionContext) {

  private val client = new GRPCClient(host = host, port = port)

  def db(name: String): Db = Db(name)

  def check: Future[HealthCheckResponse] = client.checkConnection()

  def write(bit: Bit): Future[RPCInsertResult] =
    client.write(
      RPCInsert(
        database = bit.db,
        namespace = bit.namespace,
        metric = bit.metric,
        timestamp = bit.ts getOrElse System.currentTimeMillis,
        value = bit.concreteValue match {
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

  def execute(sqlStatement: SQLStatement): Future[SQLStatementResponse] = {
    val sqlStatementRequest =
      SQLRequestStatement(db = sqlStatement.db,
                          namespace = sqlStatement.namespace,
                          statement = sqlStatement.sQLStatement)
    client.executeSQLStatement(sqlStatementRequest)
  }

  def close(): Unit = {}
}

case class Db(name: String) {

  def namespace(namespace: String): Namespace = Namespace(name, namespace)

}

case class Namespace(db: String, name: String) {

  def bit(bit: String): Bit = Bit(db = db, namespace = name, metric = bit)

  def query(queryString: String) = SQLStatement(db = db, namespace = name, sQLStatement = queryString)

}

case class SQLStatement(db: String, namespace: String, sQLStatement: String) {
  def statement(query: String): SQLStatement = copy(sQLStatement = query)
}

case class Bit(db: String,
               namespace: String,
               metric: String,
               ts: Option[Long] = None,
               private val valueDec: Option[Double] = None,
               private val valueLong: Option[Long] = None,
               dimensions: List[DimensionAPI] = List.empty[DimensionAPI]) {

  def value(v: Long): Bit = copy(valueDec = None, valueLong = Some(v))

  def value(v: Int): Bit = copy(valueDec = None, valueLong = Some(v))

  def value(v: Double): Bit = copy(valueDec = Some(v), valueLong = None)

  def value(v: java.math.BigDecimal): Bit = if (v.scale() > 0) value(v.doubleValue()) else value(v.longValue())

  def value[T](v: Option[T]): Bit = v match {
    case Some(v: Long)                 => value(v)
    case Some(v: Int)                  => value(v)
    case Some(v: Double)               => value(v)
    case Some(v: java.math.BigDecimal) => value(v)
    case _                             => this
  }

  def concreteValue: Option[AnyVal] = valueDec orElse valueLong

  def dimension(k: String, d: Long): Bit = copy(dimensions = dimensions :+ (k, d))

  def dimension(k: String, d: Int): Bit = copy(dimensions = dimensions :+ (k, d.toLong))

  def dimension(k: String, d: Double): Bit = copy(dimensions = dimensions :+ (k, d))

  def dimension(k: String, d: String): Bit = copy(dimensions = dimensions :+ (k, d))

  def dimension(k: String, d: java.math.BigDecimal): Bit =
    if (d.scale() > 0) dimension(k, d.doubleValue()) else dimension(k, d.longValue())

  def dimension[T](k: String, d: Option[T]): Bit =
    d match {
      case Some(v: java.math.BigDecimal) => dimension(k, v)
      case Some(v: Long)                 => dimension(k, v)
      case Some(v: Int)                  => dimension(k, v)
      case Some(v: Double)               => dimension(k, v)
      case Some(v: String)               => dimension(k, v)
      case _                             => this
    }

  def timestamp(v: Long): Bit = copy(ts = Some(v))
}
