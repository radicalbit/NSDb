package io.radicalbit.nsdb.web

import akka.actor.ActorRef
import akka.http.scaladsl.model.HttpResponse
import akka.http.scaladsl.model.StatusCodes._
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server._
import akka.pattern.ask
import akka.util.Timeout
import de.heikoseeberger.akkahttpjson4s.Json4sSupport
import io.radicalbit.nsdb.actors.PublisherActor.Command.RemoveQuery
import io.radicalbit.nsdb.actors.PublisherActor.Events.QueryRemoved
import io.radicalbit.nsdb.common.protocol.Bit
import io.radicalbit.nsdb.common.statement.SelectSQLStatement
import io.radicalbit.nsdb.coordinator.ReadCoordinator.{
  ExecuteStatement,
  SelectStatementExecuted,
  SelectStatementFailed
}
import io.radicalbit.nsdb.sql.parser.SQLStatementParser
import org.json4s.{DefaultFormats, JValue, jackson}
import org.json4s.jackson.Serialization

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}

trait QueryResources extends Json4sSupport {

  implicit val serialization = jackson.Serialization
  implicit val formats: DefaultFormats

  implicit val timeout: Timeout

  private def deleteQuery(id: String, publisherActor: ActorRef)(implicit ec: ExecutionContext): Future[String] = {
    (publisherActor ? RemoveQuery(id)).mapTo[QueryRemoved].map(_.quid)
  }

  case class QueryBody(namespace: String, queryString: String, from: Option[Long], to: Option[Long])

  def queryResources(publisherActor: ActorRef, readCoordinator: ActorRef)(implicit ec: ExecutionContext): Route =
    pathPrefix("query") {
      pathPrefix(JavaUUID) { id =>
        pathEnd {
          delete {
            complete(deleteQuery(id.toString, publisherActor))
          }
        }
      }
      pathEnd {
        post {
          entity(as[QueryBody]) { qb =>
            val statementOpt = (new SQLStatementParser().parse(qb.namespace, qb.queryString), qb.from, qb.to) match {
              case (Success(statement: SelectSQLStatement), Some(from), Some(to)) =>
                Some(statement.enrichWithTimeRange("timestamp", from, to))
              case (Success(statement: SelectSQLStatement), _, _) => Some(statement)
              case _                                              => None
            }
            statementOpt match {
              case Some(statement) =>
                onComplete(readCoordinator ? ExecuteStatement(statement)) {
                  case Success(SelectStatementExecuted(_, _, values: Seq[Bit])) =>
                    complete(values)
                  case Success(SelectStatementFailed(reason)) =>
                    complete(HttpResponse(InternalServerError, entity = reason))
                  case Failure(ex) => complete(HttpResponse(InternalServerError, entity = ex.getMessage))
                }
              case None => complete(HttpResponse(BadRequest, entity = s"statement ${qb.queryString} is invalid"))
            }
          }
        }
      }
    }
}
