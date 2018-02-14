package io.radicalbit.nsdb.actors

import java.io.File
import java.nio.file.Paths
import java.util.UUID
import java.util.concurrent.TimeUnit

import akka.actor.{Actor, ActorLogging, ActorRef, Props}
import akka.util.Timeout
import io.radicalbit.nsdb.actors.IndexAccumulatorActor.Refresh
import io.radicalbit.nsdb.actors.IndexPerformerActor.PerformWrites
import io.radicalbit.nsdb.common.exception.InvalidStatementException
import io.radicalbit.nsdb.common.protocol.Bit
import io.radicalbit.nsdb.index.lucene.CountAllGroupsCollector
import io.radicalbit.nsdb.index.{FacetIndex, TimeSeriesIndex}
import io.radicalbit.nsdb.protocol.MessageProtocol.Commands._
import io.radicalbit.nsdb.protocol.MessageProtocol.Events._
import io.radicalbit.nsdb.statement.StatementParser
import io.radicalbit.nsdb.statement.StatementParser.{ParsedAggregatedQuery, ParsedDeleteQuery, ParsedSimpleQuery}
import org.apache.lucene.index.{IndexNotFoundException, IndexWriter}
import org.apache.lucene.search.MatchAllDocsQuery
import org.apache.lucene.store.MMapDirectory

import scala.concurrent.ExecutionContextExecutor
import scala.concurrent.duration.FiniteDuration
import scala.util.{Failure, Success, Try}

class IndexAccumulatorActor(basePath: String, db: String, namespace: String) extends Actor with ActorLogging {
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

  var performerActor: ActorRef = _

  override def preStart(): Unit = {
    Option(Paths.get(basePath, db, namespace).toFile.list())
      .map(_.toList)
      .getOrElse(List.empty)
      .filter(f => new File(Paths.get(basePath, db, namespace, f).toString).isDirectory)
      .filterNot(m => List("metadata", "shards", "schemas").contains(m))
      .foreach { metric =>
        val directory = new MMapDirectory(Paths.get(basePath, db, namespace, metric))
        val newIndex  = new TimeSeriesIndex(directory)
        indexes += (metric -> newIndex)
      }

    performerActor =
      context.actorOf(IndexPerformerActor.props(basePath, db, namespace), s"index-performer-service-$db-$namespace")

    context.system.scheduler.schedule(interval, interval) {
      if (opBufferMap.nonEmpty && performingOps.isEmpty) {
        performingOps = opBufferMap.toMap
        performerActor ! PerformWrites(performingOps)
      }
    }

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
        implicit val iWriter: IndexWriter = i.getWriter
        i.deleteAll()
        iWriter.close()
        i.refresh()
        indexes -= metric
      }
      facetIndex.foreach { fi =>
        implicit val fiWriter: IndexWriter = fi.getWriter
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
      val hits  = Try(index.query(new MatchAllDocsQuery(), Seq.empty, Int.MaxValue, None)).getOrElse(Seq.empty)
      sender ! CountGot(db, ns, metric, hits.size)
    case ExecuteSelectStatement(statement, schema) =>
      statementParser.parseStatement(statement, schema) match {
        case Success(ParsedSimpleQuery(_, metric, q, false, limit, fields, sort)) =>
          handleQueryResults(metric, Try(getIndex(metric).query(q, fields, limit, sort)))
        case Success(ParsedSimpleQuery(_, metric, q, true, limit, fields, sort)) if fields.lengthCompare(1) == 0 =>
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

  private val opBufferMap: mutable.Map[String, Operation] = mutable.Map.empty
  private var performingOps: Map[String, Operation]       = Map.empty

  def accumulate: Receive = {
    case AddRecord(_, ns, metric, bit) =>
      opBufferMap += (UUID.randomUUID().toString -> WriteOperation(ns, metric, bit))
      sender ! RecordAdded(db, ns, metric, bit)
    case DeleteRecord(_, ns, metric, bit) =>
      opBufferMap += (UUID.randomUUID().toString -> DeleteRecordOperation(ns, metric, bit))
      sender ! RecordDeleted(db, ns, metric, bit)
    case ExecuteDeleteStatementInternal(statement, schema) =>
      statementParser.parseStatement(statement, schema) match {
        case Success(ParsedDeleteQuery(ns, metric, q)) =>
          opBufferMap += (UUID.randomUUID().toString -> DeleteQueryOperation(ns, metric, q))
          sender() ! DeleteStatementExecuted(db = db, namespace = namespace, metric = metric)
        case Failure(ex) =>
          sender() ! DeleteStatementFailed(db = db, namespace = namespace, metric = statement.metric, ex.getMessage)
      }
    case Refresh(writeIds, metrics) =>
      opBufferMap --= writeIds
      performingOps = Map.empty
      metrics.foreach { metric =>
        getIndex(metric).refresh()
      }
  }

  override def receive: Receive = {
    readOps orElse ddlOps orElse accumulate
  }
}

object IndexAccumulatorActor {

  case class Refresh(writeIds: Seq[String], metrics: Seq[String])

  def props(basePath: String, db: String, namespace: String): Props =
    Props(new IndexAccumulatorActor(basePath, db, namespace))
}
