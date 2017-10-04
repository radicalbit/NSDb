package io.radicalbit.nsdb.actors

import java.nio.file.Paths
import java.util.concurrent.TimeUnit

import akka.actor.{Actor, ActorLogging, Props, Stash}
import akka.util.Timeout
import cats.data.Validated.{Invalid, Valid}
import io.radicalbit.nsdb.actors.IndexerActor.{Accumulate, PerformWrites}
import io.radicalbit.nsdb.actors.NamespaceDataActor.commands._
import io.radicalbit.nsdb.actors.NamespaceDataActor.events._
import io.radicalbit.nsdb.common.protocol.Bit
import io.radicalbit.nsdb.coordinator.ReadCoordinator.{GetMetrics, MetricsGot}
import io.radicalbit.nsdb.coordinator.WriteCoordinator.MetricDropped
import io.radicalbit.nsdb.coordinator.{ReadCoordinator, WriteCoordinator}
import io.radicalbit.nsdb.index.TimeSeriesIndex
import io.radicalbit.nsdb.statement.StatementParser
import io.radicalbit.nsdb.statement.StatementParser.{ParsedAggregatedQuery, ParsedDeleteQuery, ParsedSimpleQuery}
import org.apache.lucene.index.{IndexNotFoundException, IndexWriter}
import org.apache.lucene.search.Query
import org.apache.lucene.store.FSDirectory

import scala.concurrent.ExecutionContextExecutor
import scala.concurrent.duration.FiniteDuration
import scala.util.{Failure, Success, Try}

sealed trait Operation {
  val ns: String
  val metric: String
}
case class DeleteRecordOperation(ns: String, metric: String, bit: Bit)    extends Operation
case class DeleteQueryOperation(ns: String, metric: String, query: Query) extends Operation
case class WriteOperation(ns: String, metric: String, bit: Bit)           extends Operation

class IndexerActor(basePath: String, namespace: String) extends Actor with ActorLogging with Stash /*with PostStart*/ {
  import scala.collection.mutable

  private val statementParser = new StatementParser()

  private val indexes: mutable.Map[String, TimeSeriesIndex] = mutable.Map.empty

  implicit val disp: ExecutionContextExecutor = context.system.dispatcher

  implicit val timeout =
    Timeout(context.system.settings.config.getDuration("nsdb.publisher.timeout", TimeUnit.SECONDS), TimeUnit.SECONDS)

  val interval = FiniteDuration(
    context.system.settings.config.getDuration("nsdb.write.scheduler.interval", TimeUnit.SECONDS),
    TimeUnit.SECONDS)

  context.system.scheduler.schedule(interval, interval) {
    self ! PerformWrites
  }

  private def getIndex(metric: String) =
    indexes.getOrElse(metric, {
      val path     = FSDirectory.open(Paths.get(basePath, namespace, metric))
      val newIndex = new TimeSeriesIndex(path)
      indexes += (metric -> newIndex)
      newIndex
    })

  private def handleQueryResults(metric: String, out: Try[Seq[Bit]]) = {
    out match {
      case Success(docs) =>
        log.debug("found {} records", docs.size)
        sender() ! ReadCoordinator.SelectStatementExecuted(namespace = namespace, metric = metric, docs)
      case Failure(_: IndexNotFoundException) =>
        log.debug("index not found")
        sender() ! ReadCoordinator.SelectStatementExecuted(namespace = namespace, metric = metric, Seq.empty)
      case Failure(ex) =>
        log.error(ex, "select statement failed")
        sender() ! ReadCoordinator.SelectStatementFailed(ex.getMessage)
    }
  }

  def ddlOps: Receive = {
    case DeleteMetric(ns, metric) =>
      val index           = getIndex(metric)
      implicit val writer = index.getWriter
      index.deleteAll()
      writer.close()
      sender ! MetricDeleted(ns, metric)
    case DeleteAllMetrics(ns) =>
      indexes.foreach {
        case (_, index) =>
          implicit val writer: IndexWriter = index.getWriter
          index.deleteAll()
          writer.close()
      }
      sender ! AllMetricsDeleted(ns)
    case WriteCoordinator.DropMetric(_, metric) =>
      indexes
        .get(metric)
        .fold {
          sender() ! MetricDropped(namespace, metric)
        } { index =>
          implicit val writer: IndexWriter = index.getWriter
          index.deleteAll()
          writer.close()
          indexes -= metric
          sender() ! MetricDropped(namespace, metric)
        }
  }

  def readOps: Receive = {
    case GetMetrics(_) =>
      sender() ! MetricsGot(namespace, indexes.keys.toSeq)
    case GetCount(ns, metric) =>
      val index = getIndex(metric)
      val hits  = index.timeRange(0, Long.MaxValue, Seq.empty)
      sender ! CountGot(ns, metric, hits.size)
    case ReadCoordinator.ExecuteSelectStatement(statement, schema) =>
      statementParser.parseStatement(statement, Some(schema)) match {
        case Success(ParsedSimpleQuery(_, metric, q, limit, fields, sort)) =>
          handleQueryResults(metric, Try(getIndex(metric).query(q, fields, limit, sort)))
        case Success(ParsedAggregatedQuery(_, metric, q, collector)) =>
          handleQueryResults(metric, Try(getIndex(metric).query(q, collector)))
        case Failure(ex) => sender() ! ReadCoordinator.SelectStatementFailed(ex.getMessage)
        case _           => sender() ! ReadCoordinator.SelectStatementFailed("Not a select statement.")
      }
  }

  private val opBufferMap: mutable.Map[String, Seq[Operation]] = mutable.Map.empty

  def accumulate: Receive = {
    case AddRecord(ns, metric, bit) =>
      opBufferMap
        .get(metric)
        .fold {
          opBufferMap += (metric -> Seq(WriteOperation(ns, metric, bit)))
        } { list =>
          opBufferMap += (metric -> (list :+ WriteOperation(ns, metric, bit)))
        }
      sender ! RecordAdded(ns, metric, bit)
    case AddRecords(ns, metric, bits) =>
      val ops = bits.map(WriteOperation(ns, metric, _))
      opBufferMap
        .get(metric)
        .fold {
          opBufferMap += (metric -> ops)
        } { list =>
          opBufferMap += (metric -> (list ++ ops))
        }
      sender ! RecordsAdded(ns, metric, bits)
    case DeleteRecord(ns, metric, bit) =>
      opBufferMap
        .get(metric)
        .fold {
          opBufferMap += (metric -> Seq(DeleteRecordOperation(ns, metric, bit)))
        } { list =>
          opBufferMap += (metric -> (list :+ DeleteRecordOperation(ns, metric, bit)))
        }
      sender ! RecordDeleted(ns, metric, bit)
    case WriteCoordinator.ExecuteDeleteStatement(statement) =>
      statementParser.parseStatement(statement) match {
        case Success(ParsedDeleteQuery(ns, metric, q)) =>
          opBufferMap
            .get(metric)
            .fold {
              opBufferMap += (metric -> Seq(DeleteQueryOperation(ns, metric, q)))
            } { list =>
              opBufferMap += (metric -> (list :+ DeleteQueryOperation(ns, metric, q)))
            }
          sender() ! WriteCoordinator.DeleteStatementExecuted(namespace = namespace, metric = metric)
        case Failure(ex) =>
          sender() ! WriteCoordinator.DeleteStatementFailed(namespace = namespace,
                                                            metric = statement.metric,
                                                            ex.getMessage)
      }
    case PerformWrites =>
      context.become(perform)
      self ! PerformWrites
  }

  def perform: Receive = {
    case PerformWrites =>
      opBufferMap.keys.foreach { metric =>
        val index                        = getIndex(metric)
        implicit val writer: IndexWriter = index.getWriter
        opBufferMap(metric).foreach {
          case WriteOperation(_, _, bit) =>
            index.write(bit)
//            w match {
//              case Valid(r)   => sender ! RecordAdded(ns, metric, bit)
//              case Invalid(l) => sender ! RecordRejected(ns, metric, bit, l.toList)
//            }
          case DeleteRecordOperation(_, _, bit) =>
            index.delete(bit)
//            sender ! RecordDeleted(ns, _, bit)
          case DeleteQueryOperation(_, _, q) =>
            val index                        = getIndex(metric)
            implicit val writer: IndexWriter = index.getWriter
            index.delete(q)
        }
        writer.flush()
        writer.close()
      }
      self ! Accumulate
    case Accumulate =>
      unstashAll()
      context.become(readOps orElse ddlOps orElse accumulate)
    case _ => stash()
  }

  override def receive: Receive = {
    readOps orElse ddlOps orElse accumulate
  }
}

object IndexerActor {

  case object PerformWrites
  case object Accumulate

  def props(basePath: String, namespace: String): Props = Props(new IndexerActor(basePath, namespace: String))

}
