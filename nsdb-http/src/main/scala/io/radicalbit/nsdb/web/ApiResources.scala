package io.radicalbit.nsdb.web

import akka.actor.ActorRef
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport
import akka.http.scaladsl.model.StatusCodes._
import akka.http.scaladsl.model.{ContentTypes, HttpEntity, HttpResponse}
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server._
import akka.pattern.ask
import akka.util.Timeout
import io.radicalbit.nsdb.actors.PublisherActor.Command.RemoveQuery
import io.radicalbit.nsdb.actors.PublisherActor.Events.QueryRemoved
import io.radicalbit.nsdb.common.JSerializable
import io.radicalbit.nsdb.common.protocol.Bit
import io.radicalbit.nsdb.common.statement.SelectSQLStatement
import io.radicalbit.nsdb.protocol.MessageProtocol.Commands.{ExecuteStatement, MapInput}
import io.radicalbit.nsdb.protocol.MessageProtocol.Events._
import io.radicalbit.nsdb.security.http.NSDBAuthProvider
import io.radicalbit.nsdb.security.model.Metric
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

case class Filter(
    dimension: String,
    value: JSerializable,
    operator: FilterOperators.Value
)

case object Filter {
  def unapply(arg: Filter): Option[(String, JSerializable, String)] =
    Some((arg.dimension, arg.value, arg.operator.toString))
}

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
          case JsString(txt) => enu.withName(txt.toUpperCase)
          case somethingElse =>
            throw DeserializationException(s"Expected a value from enum $enu instead of $somethingElse")
        }
      }
    }

  implicit val FilterOperatorFormat: RootJsonFormat[FilterOperators.Value] = enumFormat(FilterOperators)

  implicit val FilterFormat = jsonFormat3(Filter.apply)

  implicit val QbFormat = jsonFormat7(QueryBody.apply)

  implicit val BitFormat = jsonFormat3(Bit.apply)

  implicit val InsertBodyFormat = jsonFormat4(InsertBody.apply)

}

trait ApiResources {

  type FilterOperator = FilterOperators.Value

  implicit val formats: DefaultFormats

  implicit val timeout: Timeout

  private def deleteQuery(id: String, publisherActor: ActorRef)(implicit ec: ExecutionContext): Future[String] = {
    (publisherActor ? RemoveQuery(id)).mapTo[QueryRemoved].map(_.quid)
  }

  case class QueryResponse(records: Seq[Bit])

  import Formats._

  def apiResources(publisherActor: ActorRef,
                   readCoordinator: ActorRef,
                   writeCoordinator: ActorRef,
                   authProvider: NSDBAuthProvider)(implicit ec: ExecutionContext): Route =
    pathPrefix("query") {
      pathPrefix(JavaUUID) { id =>
        pathEnd {
          delete {
            complete(deleteQuery(id.toString, publisherActor))
          }
        }
      }
      post {
        entity(as[QueryBody]) { qb =>
          optionalHeaderValueByName(authProvider.headerName) { header =>
            authProvider.authorizeMetric(ent = qb, header = header, writePermission = false) {
              val statementOpt =
                (new SQLStatementParser().parse(qb.db, qb.namespace, qb.queryString), qb.from, qb.to, qb.filters) match {
                  case (Success(statement: SelectSQLStatement), Some(from), Some(to), filters) if filters.nonEmpty =>
                    Some(
                      statement
                        .enrichWithTimeRange("timestamp", from, to)
                        .addConditions(filters.getOrElse(Seq.empty).map(Filter.unapply(_).get)))
                  case (Success(statement: SelectSQLStatement), None, None, filters) if filters.nonEmpty =>
                    Some(statement
                      .addConditions(filters.getOrElse(Seq.empty).map(f => Filter.unapply(f).get)))
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
        get {
          complete("RUNNING")
        }
      }

}
