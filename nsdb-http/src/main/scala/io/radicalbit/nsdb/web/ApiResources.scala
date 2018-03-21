package io.radicalbit.nsdb.web

import akka.actor.ActorRef
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport
import akka.http.scaladsl.model.StatusCodes._
import akka.http.scaladsl.model.{ContentTypes, HttpEntity, HttpResponse}
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server._
import akka.pattern.ask
import akka.util.Timeout
import io.radicalbit.nsdb.common.JSerializable
import io.radicalbit.nsdb.common.protocol.Bit
import io.radicalbit.nsdb.common.statement.SelectSQLStatement
import io.radicalbit.nsdb.index.Schema
import io.radicalbit.nsdb.protocol.MessageProtocol.Commands._
import io.radicalbit.nsdb.protocol.MessageProtocol.Events._
import io.radicalbit.nsdb.security.http.NSDBAuthProvider
import io.radicalbit.nsdb.security.model.{Db, Metric, Namespace}
import io.radicalbit.nsdb.sql.parser.SQLStatementParser
import org.json4s.DefaultFormats
import org.json4s.jackson.Serialization.write
import spray.json._

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}

object FilterOperators extends Enumeration {
  val Equality       = Value("=")
  val GreaterThan    = Value(">")
  val GreaterOrEqual = Value(">=")
  val LessThan       = Value("<")
  val LessOrEqual    = Value("<=")
  val Like           = Value("LIKE")
}

object NullableOperators extends Enumeration {
  val IsNull    = Value("ISNULL")
  val IsNotNull = Value("ISNOTNULL")
}

sealed trait Filter
case object Filter {
  def unapply(arg: Filter): Option[(String, Option[JSerializable], String)] =
    arg match {
      case byValue: FilterByValue             => Some((byValue.dimension, Some(byValue.value), byValue.operator.toString))
      case nullableValue: FilterNullableValue => Some((nullableValue.dimension, None, nullableValue.operator.toString))
    }
}

case class FilterByValue(
    dimension: String,
    value: JSerializable,
    operator: FilterOperators.Value
) extends Filter

case class FilterNullableValue(
    dimension: String,
    operator: NullableOperators.Value
) extends Filter

case class QueryBody(db: String,
                     namespace: String,
                     metric: String,
                     queryString: String,
                     from: Option[Long],
                     to: Option[Long],
                     filters: Option[Seq[Filter]])
    extends Metric
case class InsertBody(db: String, namespace: String, metric: String, bit: Bit) extends Metric

object Formats extends DefaultJsonProtocol with SprayJsonSupport {

  implicit object JSerializableJsonFormat extends RootJsonFormat[JSerializable] {
    def write(c: JSerializable) = c match {
      case v: java.lang.Double  => JsNumber(v)
      case v: java.lang.Long    => JsNumber(v)
      case v: java.lang.Integer => JsNumber(v)
      case v                    => JsString(v.toString)
    }
    def read(value: JsValue) = value match {
      case JsNumber(v) if v.scale > 0   => new java.lang.Double(v.doubleValue)
      case JsNumber(v) if v.isValidLong => new java.lang.Long(v.longValue)
      case JsNumber(v) if v.isValidInt  => new java.lang.Integer(v.intValue)
      case JsString(v)                  => v
    }
  }

  implicit def enumFormat[T <: Enumeration](implicit enu: T): RootJsonFormat[T#Value] =
    new RootJsonFormat[T#Value] {
      def write(obj: T#Value): JsValue = JsString(obj.toString)
      def read(json: JsValue): T#Value = {
        json match {
          case JsString(txt) => enu.withName(txt.replace(" ", "").toUpperCase)
          case somethingElse =>
            throw DeserializationException(s"Expected a value from enum $enu instead of $somethingElse")
        }
      }
    }

  implicit val FilterOperatorFormat: RootJsonFormat[FilterOperators.Value]  = enumFormat(FilterOperators)
  implicit val CheckOperatorFormat: RootJsonFormat[NullableOperators.Value] = enumFormat(NullableOperators)
  implicit val FilterByValueFormat                                          = jsonFormat3(FilterByValue.apply)
  implicit val FilterNullableValueFormat                                    = jsonFormat2(FilterNullableValue.apply)

  implicit object FilterJsonFormat extends RootJsonFormat[Filter] {
    def write(a: Filter) = a match {
      case f: FilterByValue       => f.toJson
      case f: FilterNullableValue => f.toJson
    }
    def read(value: JsValue): Filter =
      value.asJsObject.fields.get("value") match {
        case Some(_) => value.convertTo[FilterByValue]
        case None    => value.convertTo[FilterNullableValue]
      }
  }

  implicit val QbFormat = jsonFormat7(QueryBody.apply)

  implicit val BitFormat = jsonFormat3(Bit.apply)

  implicit val InsertBodyFormat = jsonFormat4(InsertBody.apply)

}

trait ApiResources {

  type FilterOperator = FilterOperators.Value

  implicit val formats: DefaultFormats

  implicit val timeout: Timeout

  case class QueryResponse(records: Seq[Bit])

  case class CommandRequestDatabase(db: String)                                  extends Db
  case class CommandRequestNamespace(db: String, namespace: String)              extends Namespace
  case class CommandRequestMetric(db: String, namespace: String, metric: String) extends Metric

  sealed trait CommandResponse
  case class ShowNamespacesResponse(namespaces: Set[String]) extends CommandResponse
  case class ShowMetricsResponse(metrics: Set[String])       extends CommandResponse
  case class Field(name: String, `type`: String)
  case class DescribeMetricResponse(fields: Set[Field]) extends CommandResponse

  import Formats._

  def apiResources(publisherActor: ActorRef,
                   readCoordinator: ActorRef,
                   writeCoordinator: ActorRef,
                   authProvider: NSDBAuthProvider)(implicit ec: ExecutionContext): Route =
    pathPrefix("query") {
      post {
        entity(as[QueryBody]) { qb =>
          optionalHeaderValueByName(authProvider.headerName) { header =>
            authProvider.authorizeMetric(ent = qb, header = header, writePermission = false) {
              val statementOpt =
                (new SQLStatementParser().parse(qb.db, qb.namespace, qb.queryString), qb.from, qb.to, qb.filters) match {
                  case (Success(statement: SelectSQLStatement), Some(from), Some(to), Some(filters))
                      if filters.nonEmpty =>
                    Some(
                      statement
                        .enrichWithTimeRange("timestamp", from, to)
                        .addConditions(filters.map(f => Filter.unapply(f).get)))
                  case (Success(statement: SelectSQLStatement), None, None, Some(filters)) if filters.nonEmpty =>
                    Some(statement
                      .addConditions(filters.map(f => Filter.unapply(f).get)))
                  case (Success(statement: SelectSQLStatement), Some(from), Some(to), _) =>
                    Some(statement
                      .enrichWithTimeRange("timestamp", from, to))
                  case (Success(statement: SelectSQLStatement), _, _, _) =>
                    Some(statement)
                  case _ => None
                }
              statementOpt match {
                case Some(statement) =>
                  onComplete(readCoordinator ? ExecuteStatement(statement)) {
                    case Success(SelectStatementExecuted(_, _, _, values)) =>
                      complete(HttpEntity(ContentTypes.`application/json`, write(QueryResponse(values))))
                    case Success(SelectStatementFailed(reason, MetricNotFound(metric))) =>
                      complete(HttpResponse(NotFound, entity = reason))
                    case Success(SelectStatementFailed(reason, _)) =>
                      complete(HttpResponse(InternalServerError, entity = reason))
                    case Success(_) =>
                      complete(HttpResponse(InternalServerError, entity = "unknown response"))
                    case Failure(ex) => complete(HttpResponse(InternalServerError, entity = ex.getMessage))
                  }
                case None => complete(HttpResponse(BadRequest, entity = s"statement ${qb.queryString} is invalid"))
              }
            }
          }
        }
      }
    } ~
      pathPrefix("data") {
        post {
          entity(as[InsertBody]) { insertBody =>
            optionalHeaderValueByName(authProvider.headerName) { header =>
              authProvider.authorizeMetric(ent = insertBody, header = header, writePermission = true) {
                onComplete(
                  writeCoordinator ? MapInput(insertBody.bit.timestamp,
                                              insertBody.db,
                                              insertBody.namespace,
                                              insertBody.metric,
                                              insertBody.bit)) {
                  case Success(_: InputMapped) =>
                    complete("OK")
                  case Success(RecordRejected(_, _, _, _, reasons)) =>
                    complete(HttpResponse(BadRequest, entity = reasons.mkString(",")))
                  case Success(_) =>
                    complete(HttpResponse(InternalServerError, entity = "unknown response"))
                  case Failure(ex) => complete(HttpResponse(InternalServerError, entity = ex.getMessage))
                }
              }
            }
          }
        }
      } ~
      pathPrefix("status") {
        (pathEnd & get) {
          complete("RUNNING")
        }
      } ~
      pathPrefix("commands") {
        optionalHeaderValueByName(authProvider.headerName) { header =>
          pathPrefix(Segment) { db =>
            path("namespaces") {
              (pathEnd & get) {
                authProvider.authorizeDb(CommandRequestDatabase(db), header, false) {
                  onComplete(readCoordinator ? GetNamespaces(db)) {
                    case Success(NamespacesGot(_, namespaces)) =>
                      complete(HttpEntity(ContentTypes.`application/json`, write(ShowNamespacesResponse(namespaces))))
                    case Success(_)  => complete(HttpResponse(InternalServerError, entity = "Unknown reason"))
                    case Failure(ex) => complete(HttpResponse(InternalServerError, entity = ex.getMessage))
                  }
                }
              }
            } ~
              pathPrefix(Segment) { namespace =>
                path("metrics") {
                  pathEnd {
                    get {
                      authProvider.authorizeNamespace(CommandRequestNamespace(db, namespace), header, false) {
                        onComplete(readCoordinator ? GetMetrics(db, namespace)) {
                          case Success(MetricsGot(_, _, metrics)) =>
                            complete(HttpEntity(ContentTypes.`application/json`, write(ShowMetricsResponse(metrics))))
                          case Success(_)  => complete(HttpResponse(InternalServerError, entity = "Unknown reason"))
                          case Failure(ex) => complete(HttpResponse(InternalServerError, entity = ex.getMessage))
                        }
                      }
                    }
                  }
                } ~
                  pathEnd {
                    delete {
                      authProvider.authorizeNamespace(CommandRequestNamespace(db, namespace), header, true) {
                        onComplete(writeCoordinator ? DeleteNamespace(db, namespace)) {
                          case Success(NamespaceDeleted(_, _)) => complete("Ok")
                          case Success(_)                      => complete(HttpResponse(InternalServerError, entity = "Unknown reason"))
                          case Failure(ex)                     => complete(HttpResponse(InternalServerError, entity = ex.getMessage))
                        }
                      }
                    }
                  } ~
                  pathPrefix(Segment) { metric =>
                    (pathEnd & get) {
                      authProvider.authorizeMetric(CommandRequestMetric(db, namespace, metric), header, false) {
                        onComplete(readCoordinator ? GetSchema(db, namespace, metric)) {
                          case Success(SchemaGot(_, _, _, Some(schema))) =>
                            complete(
                              HttpEntity(
                                ContentTypes.`application/json`,
                                write(
                                  DescribeMetricResponse(
                                    schema.fields
                                      .map(field =>
                                        Field(name = field.name, `type` = field.indexType.getClass.getSimpleName))
                                  )
                                )
                              )
                            )
                          case Success(SchemaGot(_, _, _, None)) =>
                            complete(HttpResponse(NotFound))
                          case Failure(ex) => complete(HttpResponse(InternalServerError, entity = ex.getMessage))
                          case _           => complete(HttpResponse(InternalServerError, entity = "Unknown reason"))
                        }
                      }
                    } ~
                      delete {
                        authProvider.authorizeMetric(CommandRequestMetric(db, namespace, metric), header, true) {
                          onComplete(writeCoordinator ? DropMetric(db, namespace, metric)) {
                            case Success(MetricDropped(_, _, _)) => complete("Ok")
                            case Success(_)                      => complete(HttpResponse(InternalServerError, entity = "Unknown reason"))
                            case Failure(ex)                     => complete(HttpResponse(InternalServerError, entity = ex.getMessage))
                          }
                        }
                      }
                  }
              }
          }
        }
      }
}
