package io.radicalbit.nsdb.actors

import java.nio.file.Paths
import java.util.concurrent.TimeUnit

import akka.actor.{Actor, ActorLogging, Props, Stash}
import akka.util.Timeout
import cats.data.Validated.{Invalid, Valid}
import io.radicalbit.nsdb.actors.IndexerActor._
import io.radicalbit.nsdb.common.exception.InvalidStatementException
import io.radicalbit.nsdb.common.protocol.Bit
import io.radicalbit.nsdb.index.lucene.CountAllGroupsCollector
import io.radicalbit.nsdb.index.{FacetIndex, TimeSeriesIndex}
import io.radicalbit.nsdb.protocol.MessageProtocol.Commands._
import io.radicalbit.nsdb.protocol.MessageProtocol.Events._
import io.radicalbit.nsdb.statement.StatementParser
import io.radicalbit.nsdb.statement.StatementParser.{ParsedAggregatedQuery, ParsedDeleteQuery, ParsedSimpleQuery}
import org.apache.lucene.facet.taxonomy.directory.DirectoryTaxonomyWriter
import org.apache.lucene.index.{IndexNotFoundException, IndexWriter}
import org.apache.lucene.search.{MatchAllDocsQuery, Query}
import org.apache.lucene.store.MMapDirectory

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

class IndexerActor(basePath: String, db: String, namespace: String) extends Actor with ActorLogging with Stash {
  import scala.collection.mutable

  private val statementParser = new StatementParser()

  private val indexes: mutable.Map[String, TimeSeriesIndex] = mutable.Map.empty

  private val facetIndexes: mutable.Map[String, FacetIndex] = mutable.Map.empty

  implicit val dispatcher: ExecutionContextExecutor = context.system.dispatcher

  implicit val timeout: Timeout =
    Timeout(context.system.settings.config.getDuration("nsdb.publisher.timeout", TimeUnit.SECONDS), TimeUnit.SECONDS)

  lazy val interval = FiniteDuration(
    context.system.settings.config.getDuration("nsdb.write.scheduler.interval", TimeUnit.SECONDS),
    TimeUnit.SECONDS)

  context.system.scheduler.schedule(interval, interval) {
    if (opBufferMap.nonEmpty)
      self ! PerformWrites
  }

  private def getIndex(metric: String) =
    indexes.getOrElse(
      metric, {
        val directory = new MMapDirectory(Paths.get(basePath, db, namespace, metric))
        val newIndex  = new TimeSeriesIndex(directory)
        indexes += (metric -> newIndex)
        newIndex
      }
    )

  private def getFacetIndex(metric: String) =
    facetIndexes.getOrElse(
      metric, {
        val directory     = new MMapDirectory(Paths.get(basePath, db, namespace, metric, "facet"))
        val taxoDirectory = new MMapDirectory(Paths.get(basePath, db, namespace, metric, "facet", "taxo"))
        val newIndex      = new FacetIndex(directory, taxoDirectory)
        facetIndexes += (metric -> newIndex)
        newIndex
      }
    )

  private def handleQueryResults(metric: String, out: Try[Seq[Bit]]): Unit = {
    out match {
      case Success(docs) =>
        log.debug("found {} records", docs.size)
        sender() ! SelectStatementExecuted(db = db, namespace = namespace, metric = metric, docs)
      case Failure(_: IndexNotFoundException) =>
        log.debug("index not found")
        sender() ! SelectStatementExecuted(db = db, namespace = namespace, metric = metric, Seq.empty)
      case Failure(ex) =>
        log.error(ex, "select statement failed")
        sender() ! SelectStatementFailed(ex.getMessage)
    }
  }

  def ddlOps: Receive = {
    case DeleteAllMetrics(_, ns) =>
      indexes.foreach {
        case (_, index) =>
          implicit val iWriter: IndexWriter = index.getWriter
          index.deleteAll()
          iWriter.close()
      }
      facetIndexes.foreach {
        case (k, index) =>
          implicit val iWriter: IndexWriter = index.getWriter
          index.deleteAll()
          iWriter.close()
          facetIndexes -= k
      }
      sender ! AllMetricsDeleted(db, ns)
    case DropMetric(_, _, metric) =>
      opBufferMap -= metric
      val index      = indexes.get(metric)
      val facetIndex = facetIndexes.get(metric)

      index.foreach { i =>
        implicit val iWriter = i.getWriter
        i.deleteAll()
        iWriter.close()
        i.refresh()
        indexes -= metric
      }
      facetIndex.foreach { fi =>
        implicit val fiWriter = fi.getWriter
        fi.deleteAll()
        fiWriter.close()
        fi.refresh()
        facetIndexes -= metric
      }

      sender() ! MetricDropped(db, namespace, metric)
  }

  def readOps: Receive = {
    case GetMetrics(_, _) =>
      sender() ! MetricsGot(db, namespace, indexes.keys.toSet)
    case GetCount(_, ns, metric) =>
      val index = getIndex(metric)
      val hits  = index.query(new MatchAllDocsQuery(), Seq.empty, Int.MaxValue, None)
      sender ! CountGot(db, ns, metric, hits.size)
    case ExecuteSelectStatement(statement, schema) =>
      statementParser.parseStatement(statement, schema) match {
        case Success(ParsedSimpleQuery(_, metric, q, false, limit, fields, sort)) =>
          handleQueryResults(metric, Try(getIndex(metric).query(q, fields, limit, sort)))
        case Success(ParsedSimpleQuery(_, metric, q, true, limit, fields, sort)) if fields.size == 1 =>
          handleQueryResults(metric,
                             Try(getFacetIndex(metric).getDistinctField(q, fields.map(_.name).head, sort, limit)))
        case Success(ParsedAggregatedQuery(_, metric, q, collector: CountAllGroupsCollector, sort, limit)) =>
          handleQueryResults(metric, Try(getFacetIndex(metric).getCount(q, collector.groupField, sort, limit)))
        case Success(ParsedAggregatedQuery(_, metric, q, collector, sort, limit)) =>
          handleQueryResults(metric, Try(getIndex(metric).query(q, collector, limit, sort)))
        case Failure(ex: InvalidStatementException) => sender() ! SelectStatementFailed(ex.message)
        case _                                      => sender() ! SelectStatementFailed("Not a select statement.")
      }
  }

  private val opBufferMap: mutable.Map[String, Seq[Operation]] = mutable.Map.empty

  def accumulate: Receive = {
    case AddRecord(_, ns, metric, bit) =>
      opBufferMap
        .get(metric)
        .fold {
          opBufferMap += (metric -> Seq(WriteOperation(ns, metric, bit)))
        } { list =>
          opBufferMap += (metric -> (list :+ WriteOperation(ns, metric, bit)))
        }
      sender ! RecordAdded(db, ns, metric, bit)
    case AddRecords(_, ns, metric, bits) =>
      val ops = bits.map(WriteOperation(ns, metric, _))
      opBufferMap
        .get(metric)
        .fold {
          opBufferMap += (metric -> ops)
        } { list =>
          opBufferMap += (metric -> (list ++ ops))
        }
      sender ! RecordsAdded(db, ns, metric, bits)
    case DeleteRecord(_, ns, metric, bit) =>
      opBufferMap
        .get(metric)
        .fold {
          opBufferMap += (metric -> Seq(DeleteRecordOperation(ns, metric, bit)))
        } { list =>
          opBufferMap += (metric -> (list :+ DeleteRecordOperation(ns, metric, bit)))
        }
      sender ! RecordDeleted(db, ns, metric, bit)
    case ExecuteDeleteStatementInternal(statement, schema) =>
      statementParser.parseStatement(statement, schema) match {
        case Success(ParsedDeleteQuery(ns, metric, q)) =>
          opBufferMap
            .get(metric)
            .fold {
              opBufferMap += (metric -> Seq(DeleteQueryOperation(ns, metric, q)))
            } { list =>
              opBufferMap += (metric -> (list :+ DeleteQueryOperation(ns, metric, q)))
            }
          sender() ! DeleteStatementExecuted(db = db, namespace = namespace, metric = metric)
        case Failure(ex) =>
          sender() ! DeleteStatementFailed(db = db, namespace = namespace, metric = statement.metric, ex.getMessage)
      }
    case PerformWrites =>
      context.become(perform)
      self ! PerformWrites
  }

  def perform: Receive = {
    case PerformWrites =>
      opBufferMap.keys.foreach { metric =>
        val index                               = getIndex(metric)
        val facetIndex                          = getFacetIndex(metric)
        implicit val writer: IndexWriter        = index.getWriter
        val facetWriter: IndexWriter            = facetIndex.getWriter
        val taxoWriter: DirectoryTaxonomyWriter = facetIndex.getTaxoWriter
        opBufferMap(metric).foreach {
          case WriteOperation(_, _, bit) =>
            index.write(bit).map(_ => facetIndex.write(bit)(facetWriter, taxoWriter)) match {
              case Valid(_)      =>
              case Invalid(errs) => log.error(errs.toList.mkString(","))
            }
          //TODO handle errors
          case DeleteRecordOperation(_, _, bit) =>
            index.delete(bit)
            facetIndex.delete(bit)(facetWriter)
          case DeleteQueryOperation(_, _, q) =>
            val index = getIndex(metric)
            index.delete(q)
        }
        writer.close()
        taxoWriter.close()
        facetWriter.close()
        facetIndex.refresh()
        index.refresh()
      }
      opBufferMap.clear()
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

  def props(basePath: String, db: String, namespace: String): Props = Props(new IndexerActor(basePath, db, namespace))
}
