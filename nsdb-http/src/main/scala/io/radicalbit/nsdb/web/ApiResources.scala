package io.radicalbit.nsdb.web

import akka.actor.ActorRef
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport
import akka.http.scaladsl.model.StatusCodes._
import akka.http.scaladsl.model.{ContentTypes, HttpEntity, HttpResponse}
import akka.http.scaladsl.server.Directives.{entity, _}
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
import io.radicalbit.nsdb.sql.parser.SQLStatementParser
import org.json4s.DefaultFormats
import org.json4s.jackson.Serialization.write
import spray.json._

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}

case class QueryBody(namespace: String, queryString: String, from: Option[Long], to: Option[Long])
case class InsertBody(namespace: String, metric: String, bit: Bit)

object Formats extends DefaultJsonProtocol with SprayJsonSupport {
  implicit val QbFormat = jsonFormat4(QueryBody.apply)

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

  implicit val BitFormat = jsonFormat3(Bit.apply)

  implicit val InsertBodyFormat = jsonFormat3(InsertBody.apply)

}

trait ApiResources {

  implicit val formats: DefaultFormats

  implicit val timeout: Timeout

  private def deleteQuery(id: String, publisherActor: ActorRef)(implicit ec: ExecutionContext): Future[String] = {
    (publisherActor ? RemoveQuery(id)).mapTo[QueryRemoved].map(_.quid)
  }

  case class QueryResponse(records: Seq[Bit])

  import Formats._

  def apiResources(publisherActor: ActorRef, readCoordinator: ActorRef, writeCoordinator: ActorRef)(
      implicit ec: ExecutionContext): Route =
    pathPrefix("query") {
      pathPrefix(JavaUUID) { id =>
        pathEnd {
          delete {
            complete(deleteQuery(id.toString, publisherActor))
          }
        }
      }
      path(Segment) { db =>
        post {
          entity(as[QueryBody]) { qb =>
            val statementOpt =
              (new SQLStatementParser().parse(db, qb.namespace, qb.queryString), qb.from, qb.to) match {
                case (Success(statement: SelectSQLStatement), Some(from), Some(to)) =>
                  Some(statement.enrichWithTimeRange("timestamp", from, to))
                case (Success(statement: SelectSQLStatement), _, _) => Some(statement)
                case _                                              => None
              }
            statementOpt match {
              case Some(statement) =>
                onComplete(readCoordinator ? ExecuteStatement(statement)) {
                  case Success(SelectStatementExecuted(_, _, _, values)) =>
                    complete(HttpEntity(ContentTypes.`application/json`, write(QueryResponse(values))))
                  case Success(SelectStatementFailed(reason)) =>
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
    } ~
      pathPrefix("data") {
        path(Segment) { db =>
          post {
            entity(as[InsertBody]) { insertBody =>
              onComplete(
                writeCoordinator ? MapInput(insertBody.bit.timestamp,
                                            db,
                                            insertBody.namespace,
                                            insertBody.metric,
                                            insertBody.bit)) {
                case Success(_: InputMapped) =>
                  complete("OK")
                case Success(RecordRejected(_, _, _, _, reasons)) =>
                  complete(HttpResponse(InternalServerError, entity = reasons.mkString(",")))
                case Success(_) =>
                  complete(HttpResponse(InternalServerError, entity = "unknown response"))
                case Failure(ex) => complete(HttpResponse(InternalServerError, entity = ex.getMessage))
              }
            }
          }
        }
      }

}
