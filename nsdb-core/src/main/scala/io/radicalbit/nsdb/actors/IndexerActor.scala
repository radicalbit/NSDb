package io.radicalbit.nsdb.actors

import java.nio.file.Paths

import akka.actor.{Actor, Props}
import io.radicalbit.nsdb.coordinator.ReadCoordinator
import io.radicalbit.nsdb.index.TimeSeriesIndex
import io.radicalbit.nsdb.model.Record
import io.radicalbit.nsdb.statement.StatementParser
import org.apache.lucene.index.IndexNotFoundException
import org.apache.lucene.store.FSDirectory

import scala.util.{Failure, Success, Try}

class IndexerActor(basePath: String) extends Actor {
  import io.radicalbit.nsdb.actors.IndexerActor._

  import scala.collection.mutable

  private val statementParser = new StatementParser()

  private val indexes: mutable.Map[String, TimeSeriesIndex] = mutable.Map.empty

  private def getIndex(metric: String) =
    indexes.getOrElse(metric, {
      val path     = FSDirectory.open(Paths.get(basePath, metric))
      val newIndex = new TimeSeriesIndex(path)
      indexes + (metric -> newIndex)
      newIndex
    })

  override def receive: Receive = {
    case AddRecord(metric, record) =>
      val index           = getIndex(metric)
      implicit val writer = index.getWriter
      index.write(record)
      writer.flush()
      writer.close()
      sender ! RecordAdded(metric, record)
    case AddRecords(metric, records) =>
      val index           = getIndex(metric)
      implicit val writer = index.getWriter
      records.foreach(index.write)
      writer.flush()
      writer.close()
      sender ! RecordsAdded(metric, records)
    case DeleteRecord(metric, record) =>
      val index           = getIndex(metric)
      implicit val writer = index.getWriter
      index.delete(record)
      writer.flush()
      writer.close()
      sender ! RecordDeleted(metric, record)
    case DeleteMetric(metric) =>
      val index           = getIndex(metric)
      implicit val writer = index.getWriter
      index.deleteAll()
      writer.close()
      sender ! MetricDeleted(metric)
    case GetCount(metric) =>
      val index = getIndex(metric)
      val hits  = index.timeRange(0, Long.MaxValue)
      sender ! CountGot(metric, hits.size)
    case ReadCoordinator.ExecuteSelectStatement(statement, schema) =>
      val queryResult = statementParser.parseStatement(statement, schema).get
      Try { getIndex(statement.metric).query(queryResult.q, queryResult.limit, queryResult.sort) } match {
        case Success(docs)                      => sender() ! ReadCoordinator.SelectStatementExecuted(docs)
        case Failure(_: IndexNotFoundException) => sender() ! ReadCoordinator.SelectStatementExecuted(Seq.empty)
        case Failure(ex)                        => sender() ! ReadCoordinator.SelectStatementFailed(ex.getMessage)
      }
  }
}

object IndexerActor {

  def props(basePath: String): Props = Props(new IndexerActor(basePath))

  case class AddRecord(metric: String, record: Record)
  case class AddRecords(metric: String, records: Seq[Record])
  case class DeleteRecord(metric: String, record: Record)
  case class DeleteMetric(metric: String)
  case class GetCount(metric: String)
  case class CountGot(metric: String, count: Int)
  case class RecordAdded(metric: String, record: Record)
  case class RecordsAdded(metric: String, record: Seq[Record])
  case class RecordRejected(metric: String, record: Record, reasons: List[String])
  case class RecordDeleted(metric: String, record: Record)
  case class MetricDeleted(metric: String)
}
