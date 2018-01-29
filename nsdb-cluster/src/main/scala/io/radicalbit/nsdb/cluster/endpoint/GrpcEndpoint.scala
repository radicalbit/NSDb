package io.radicalbit.nsdb.cluster.endpoint

import java.util.concurrent.TimeUnit

import akka.actor.{ActorRef, ActorSystem}
import akka.pattern.ask
import akka.util.Timeout
import io.radicalbit.nsdb.client.rpc.GRPCServer
import io.radicalbit.nsdb.common.JSerializable
import io.radicalbit.nsdb.common.exception.InvalidStatementException
import io.radicalbit.nsdb.common.protocol.{Bit, MetricField}
import io.radicalbit.nsdb.common.statement._
import io.radicalbit.nsdb.protocol.MessageProtocol.Commands._
import io.radicalbit.nsdb.protocol.MessageProtocol.Events._
import io.radicalbit.nsdb.rpc.common.{Dimension, Bit => GrpcBit}
import io.radicalbit.nsdb.rpc.request.RPCInsert
import io.radicalbit.nsdb.rpc.requestCommand.{DescribeMetric, ShowMetrics, ShowNamespaces}
import io.radicalbit.nsdb.rpc.requestSQL.SQLRequestStatement
import io.radicalbit.nsdb.rpc.response.RPCInsertResult
import io.radicalbit.nsdb.rpc.responseCommand.{
  Namespaces,
  MetricSchemaRetrieved => GrpcMetricSchemaRetrieved,
  MetricsGot => GrpcMetricsGot
}
import io.radicalbit.nsdb.rpc.responseSQL.SQLStatementResponse
import io.radicalbit.nsdb.rpc.service.NSDBServiceCommandGrpc.NSDBServiceCommand
import io.radicalbit.nsdb.rpc.service.NSDBServiceSQLGrpc.NSDBServiceSQL
import io.radicalbit.nsdb.sql.parser.SQLStatementParser
import org.slf4j.LoggerFactory

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}
class GrpcEndpoint(readCoordinator: ActorRef, writeCoordinator: ActorRef)(implicit system: ActorSystem)
    extends GRPCServer {

  private val log = LoggerFactory.getLogger(classOf[GrpcEndpoint])

  implicit val timeout: Timeout =
    Timeout(system.settings.config.getDuration("nsdb.rpc-endpoint.timeout", TimeUnit.SECONDS), TimeUnit.SECONDS)
  implicit val sys = system.dispatcher

  log.info("Starting GrpcEndpoint")

  override protected[this] val executionContextExecutor = implicitly[ExecutionContext]

  override protected[this] def serviceSQL = GrpcEndpointServiceSQL

  override protected[this] def serviceCommand = GrpcEndpointServiceCommand

  override protected[this] val port: Int = 7817

  override protected[this] val parserSQL = new SQLStatementParser

  val innerServer = start()

  log.info("GrpcEndpoint started on port {}", port)

  protected[this] object GrpcEndpointServiceCommand extends NSDBServiceCommand {
    override def showMetrics(
        request: ShowMetrics
    ): Future[GrpcMetricsGot] = {
      //TODO: add failure handling
      log.debug("Received command ShowMetrics for namespace {}", request.namespace)
      (readCoordinator ? GetMetrics(request.db, request.namespace)).mapTo[MetricsGot].map {
        case MetricsGot(db, namespace, metrics) =>
          GrpcMetricsGot(db, namespace, metrics.toList, completedSuccessfully = true)
      }
    }

    override def describeMetric(
        request: DescribeMetric
    ): Future[GrpcMetricSchemaRetrieved] = {
      //TODO: add failure handling
      log.debug("Received command DescribeMetric for metric {}", request.metric)
      (readCoordinator ? GetSchema(db = request.db, namespace = request.namespace, metric = request.metric))
        .mapTo[SchemaGot]
        .map {
          case SchemaGot(db, namespace, metric, schema) =>
            val fields = schema
              .map(
                _.fields.map(field => MetricField(name = field.name, `type` = field.indexType.getClass.getSimpleName)))
              .getOrElse(List.empty[MetricField])
            GrpcMetricSchemaRetrieved(db,
                                      namespace,
                                      metric,
                                      fields.map(f => GrpcMetricSchemaRetrieved.MetricField(f.name, f.`type`)),
                                      completedSuccessfully = true)
        }
    }

    override def showNamespaces(
        request: ShowNamespaces
    ): Future[Namespaces] = {
      log.debug("Received command ShowNamespaces for db: {}", request.db)
      (readCoordinator ? GetNamespaces(db = request.db))
        .mapTo[NamespacesGot]
        .map { namespaces =>
          Namespaces(db = namespaces.db, namespaces = namespaces.namespaces.toSeq)
        }
        .recoverWith {
          case t =>
            Future.successful(
              Namespaces(db = request.db, completedSuccessfully = false, errors = "Cluster unable to fulfill request"))
        }
    }
  }

  protected[this] object GrpcEndpointServiceSQL extends NSDBServiceSQL {

    override def insertBit(request: RPCInsert): Future[RPCInsertResult] = {
      log.debug("Received a write request {}", request)

      val res = (writeCoordinator ? MapInput(
        db = request.database,
        namespace = request.namespace,
        metric = request.metric,
        ts = request.timestamp,
        record = Bit(timestamp = request.timestamp, dimensions = request.dimensions.map {
          case (k, v) => (k, dimensionFor(v.value))
        }, value = valueFor(request.value))
      )).mapTo[InputMapped] map (_ => RPCInsertResult(completedSuccessfully = true)) recover {
        case t => RPCInsertResult(completedSuccessfully = false, t.getMessage)
      }

      res.onComplete {
        case Success(res: RPCInsertResult) =>
          log.debug("Completed the write request {}", request)
          if (res.completedSuccessfully)
            log.debug("The result is {}", res)
          else
            log.error("The result is {}", res)
        case Failure(t: Throwable) =>
          log.error(s"error on request $request", t)
      }
      res
    }

    private def valueFor(v: RPCInsert.Value): JSerializable = v match {
      case _: RPCInsert.Value.DecimalValue => v.decimalValue.get
      case _: RPCInsert.Value.LongValue    => v.longValue.get
    }

    private def dimensionFor(v: Dimension.Value): JSerializable = v match {
      case _: Dimension.Value.DecimalValue => v.decimalValue.get
      case _: Dimension.Value.LongValue    => v.longValue.get
      case _                               => v.stringValue.get
    }

    private def toGrpcBit(bit: Bit): GrpcBit = {
      GrpcBit(
        timestamp = bit.timestamp,
        value = bit.value match {
          case v: java.lang.Long    => GrpcBit.Value.LongValue(v)
          case v: java.lang.Double  => GrpcBit.Value.DecimalValue(v)
          case v: java.lang.Float   => GrpcBit.Value.DecimalValue(v.doubleValue())
          case v: java.lang.Integer => GrpcBit.Value.LongValue(v.longValue())
        },
        dimensions = bit.dimensions.map {
          case (k, v: java.lang.Double)  => (k, Dimension(Dimension.Value.DecimalValue(v)))
          case (k, v: java.lang.Float)   => (k, Dimension(Dimension.Value.DecimalValue(v.doubleValue())))
          case (k, v: java.lang.Long)    => (k, Dimension(Dimension.Value.LongValue(v)))
          case (k, v: java.lang.Integer) => (k, Dimension(Dimension.Value.LongValue(v.longValue())))
          case (k, v)                    => (k, Dimension(Dimension.Value.StringValue(v.toString)))
        }
      )
    }

    override def executeSQLStatement(
        request: SQLRequestStatement
    ): Future[SQLStatementResponse] = {
      val requestDb        = request.db
      val requestNamespace = request.namespace
      val sqlStatement     = parserSQL.parse(request.db, request.namespace, request.statement)
      sqlStatement match {
        // Parsing Success
        case Success(statement) =>
          statement match {
            case select: SelectSQLStatement =>
              log.debug("Received a select request {}", select)
              (readCoordinator ? ExecuteStatement(select))
                .map {
                  // SelectExecution Success
                  case SelectStatementExecuted(db, namespace, metric, values: Seq[Bit]) =>
                    log.debug("SQL statement succeeded on db {} with namespace {} and metric {}",
                              db,
                              namespace,
                              metric)
                    SQLStatementResponse(
                      db = db,
                      namespace = namespace,
                      metric = metric,
                      completedSuccessfully = true,
                      records = values.map(toGrpcBit)
                    )
                  // SelectExecution Failure
                  case SelectStatementFailed(reason, _) =>
                    SQLStatementResponse(
                      db = requestDb,
                      namespace = requestNamespace,
                      completedSuccessfully = false,
                      reason = reason
                    )

                }
                .recoverWith {
                  case t =>
                    log.error("", t)
                    Future.successful(
                      SQLStatementResponse(
                        db = requestDb,
                        namespace = requestNamespace,
                        completedSuccessfully = false,
                        reason = t.getMessage
                      ))
                }
            case insert: InsertSQLStatement =>
              log.debug("Received a insert request {}", insert)
              val result = InsertSQLStatement
                .unapply(insert)
                .map {
                  case (db, namespace, metric, ts, dimensions, value) =>
                    val timestamp = ts getOrElse System.currentTimeMillis
                    writeCoordinator ? MapInput(timestamp,
                                                db,
                                                namespace,
                                                metric,
                                                Bit(timestamp = timestamp,
                                                    value = value,
                                                    dimensions = dimensions.map(_.fields).getOrElse(Map.empty)))
                }
                .getOrElse(Future(throw new InvalidStatementException("The insert SQL statement is invalid.")))
              result
                .map {
                  case InputMapped(db, namespace, metric, record) =>
                    SQLStatementResponse(db = db,
                                         namespace = namespace,
                                         metric = metric,
                                         completedSuccessfully = true,
                                         records = Seq(toGrpcBit(record)))
                  case x: RecordRejected =>
                    SQLStatementResponse(db = x.db,
                                         namespace = x.namespace,
                                         metric = x.metric,
                                         completedSuccessfully = false,
                                         reason = x.reasons.mkString(","))
                }

            case delete: DeleteSQLStatement =>
              (writeCoordinator ? ExecuteDeleteStatement(delete))
                .mapTo[DeleteStatementExecuted]
                .map(
                  x =>
                    SQLStatementResponse(db = x.db,
                                         namespace = x.namespace,
                                         metric = x.metric,
                                         completedSuccessfully = true,
                                         records = Seq.empty))
                .recoverWith {
                  case t =>
                    Future.successful(
                      SQLStatementResponse(
                        db = requestDb,
                        namespace = requestNamespace,
                        completedSuccessfully = false,
                        reason = t.getMessage
                      ))
                }

            case drop: DropSQLStatement =>
              (writeCoordinator ? DropMetric(statement.db, statement.namespace, statement.metric))
                .mapTo[MetricDropped]
                .map(
                  x =>
                    SQLStatementResponse(db = x.db,
                                         namespace = x.namespace,
                                         metric = x.metric,
                                         completedSuccessfully = true,
                                         records = Seq.empty))
                .recoverWith {
                  case t =>
                    Future.successful(
                      SQLStatementResponse(
                        db = requestDb,
                        namespace = requestNamespace,
                        completedSuccessfully = false,
                        reason = t.getMessage
                      ))
                }
          }

        //Parsing Failure
        case Failure(exception: InvalidStatementException) =>
          Future.successful(
            SQLStatementResponse(db = request.db,
                                 namespace = request.namespace,
                                 completedSuccessfully = false,
                                 reason = "sql statement not valid",
                                 message = exception.message)
          )
        case Failure(ex) =>
          Future.successful(
            SQLStatementResponse(db = request.db,
                                 namespace = request.namespace,
                                 completedSuccessfully = false,
                                 reason = "internal error",
                                 message = ex.getMessage)
          )
      }
    }
  }
}
