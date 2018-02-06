package io.radicalbit.nsdb.cluster.actor

import java.nio.file.Paths
import java.util.concurrent.TimeUnit

import akka.actor.{Actor, ActorLogging, Props, Stash}
import akka.util.Timeout
import cats.data.Validated.{Invalid, Valid}
import io.radicalbit.nsdb.actors._
import io.radicalbit.nsdb.cluster.actor.NamespaceDataActor.{AddRecordToLocation, DeleteRecordFromLocation}
import io.radicalbit.nsdb.cluster.actor.ShardActor.{Accumulate, PerformWrites}
import io.radicalbit.nsdb.common.JSerializable
import io.radicalbit.nsdb.common.exception.InvalidStatementException
import io.radicalbit.nsdb.common.protocol.Bit
import io.radicalbit.nsdb.common.statement.{DescOrderOperator, SelectSQLStatement}
import io.radicalbit.nsdb.index.lucene._
import io.radicalbit.nsdb.index.{FacetIndex, NumericType, Schema, TimeSeriesIndex}
import io.radicalbit.nsdb.protocol.MessageProtocol.Commands._
import io.radicalbit.nsdb.protocol.MessageProtocol.Events._
import io.radicalbit.nsdb.statement.StatementParser._
import io.radicalbit.nsdb.statement.{StatementParser, TimeRangeExtractor}
import org.apache.lucene.facet.taxonomy.directory.DirectoryTaxonomyWriter
import org.apache.lucene.index.{IndexNotFoundException, IndexWriter}
import org.apache.lucene.search.MatchAllDocsQuery
import org.apache.lucene.store.MMapDirectory
import spire.implicits._
import spire.math.Interval

import scala.collection.mutable.ListBuffer
import scala.concurrent.ExecutionContextExecutor
import scala.concurrent.duration.FiniteDuration
import scala.util.{Failure, Success, Try}

case class ShardKey(metric: String, from: Long, to: Long)

class ShardActor(basePath: String, db: String, namespace: String) extends Actor with ActorLogging with Stash {
  import scala.collection.mutable

  private val statementParser = new StatementParser()

  val shards: mutable.Map[ShardKey, TimeSeriesIndex] = mutable.Map.empty

  val facetIndexShards: mutable.Map[ShardKey, FacetIndex] = mutable.Map.empty

  implicit val dispatcher: ExecutionContextExecutor = context.system.dispatcher

  implicit val timeout: Timeout =
    Timeout(context.system.settings.config.getDuration("nsdb.publisher.timeout", TimeUnit.SECONDS), TimeUnit.SECONDS)

  lazy val interval = FiniteDuration(
    context.system.settings.config.getDuration("nsdb.write.scheduler.interval", TimeUnit.SECONDS),
    TimeUnit.SECONDS)

  context.system.scheduler.schedule(interval, interval) {
    self ! PerformWrites
  }

  private def getMetricShards(metric: String)      = shards.filter(_._1.metric == metric)
  private def getMetricFacetShards(metric: String) = facetIndexShards.filter(_._1.metric == metric)

  private def getIndex(key: ShardKey) =
    shards.getOrElse(
      key, {
        val directory =
          new MMapDirectory(Paths.get(basePath, db, namespace, "shards", s"${key.metric}_${key.from}_${key.to}"))
        val newIndex = new TimeSeriesIndex(directory)
        shards += (key -> newIndex)
        newIndex
      }
    )

  private def getFacetIndex(key: ShardKey) =
    facetIndexShards.getOrElse(
      key, {
        val directory =
          new MMapDirectory(
            Paths.get(basePath, db, namespace, "shards", s"${key.metric}_${key.from}_${key.to}", "facet"))
        val taxoDirectory = new MMapDirectory(
          Paths.get(basePath, db, namespace, "shards", s"${key.metric}_${key.from}_${key.to}", "facet", "taxo"))
        val newIndex = new FacetIndex(directory, taxoDirectory)
        facetIndexShards += (key -> newIndex)
        newIndex
      }
    )

  private def handleQueryResults(metric: String, out: Try[Seq[Bit]]) = {
    out.recoverWith {
      case _: IndexNotFoundException => Success(Seq.empty)
    }
  }

  private def applyOrderingWithLimit(shardResult: Try[Seq[Bit]], statement: SelectSQLStatement, schema: Schema) = {
    Try(shardResult.get).map(s => {
      val maybeSorted = if (statement.order.isDefined) {
        val o = schema.fields.find(_.name == statement.order.get.dimension).get.indexType.ord
        implicit val ord: Ordering[JSerializable] =
          if (statement.order.get.isInstanceOf[DescOrderOperator]) o.reverse else o
        s.sortBy(_.dimensions(statement.order.get.dimension))
      } else s

      if (statement.limit.isDefined) maybeSorted.take(statement.limit.get.value) else maybeSorted
    })
  }

  override def preStart: Unit = {
    Option(Paths.get(basePath, db, namespace, "shards").toFile.list())
      .map(_.toSet)
      .getOrElse(Set.empty)
      .filter(_.split("_").length == 3)
      .map(_.split("_"))
      .foreach {
        case Array(metric, from, to) =>
          val directory =
            new MMapDirectory(Paths.get(basePath, db, namespace, "shards", s"${metric}_${from}_$to"))
          val newIndex = new TimeSeriesIndex(directory)
          shards += (ShardKey(metric, from.toLong, to.toLong) -> newIndex)
          val directoryFacets =
            new MMapDirectory(Paths.get(basePath, db, namespace, "shards", s"${metric}_${from}_${to}", "facet"))
          val taxoDirectoryFacets = new MMapDirectory(
            Paths.get(basePath, db, namespace, "shards", s"${metric}_${from}_${to}", "facet", "taxo"))
          val newFacetIndex = new FacetIndex(directoryFacets, taxoDirectoryFacets)
          facetIndexShards += (ShardKey(metric, from.toLong, to.toLong) -> newFacetIndex)
      }
  }

  def ddlOps: Receive = {
    case DeleteAllMetrics(_, ns) =>
      shards.foreach {
        case (_, index) =>
          implicit val writer: IndexWriter = index.getWriter
          index.deleteAll()
          writer.close()
          index.refresh()
      }
      facetIndexShards.foreach {
        case (k, index) =>
          implicit val writer: IndexWriter = index.getWriter
          index.deleteAll()
          writer.close()
          facetIndexShards -= k
      }
      sender ! AllMetricsDeleted(db, ns)
    case DropMetric(_, _, metric) =>
      getMetricShards(metric).foreach {
        case (key, index) =>
          implicit val writer: IndexWriter = index.getWriter
          index.deleteAll()
          writer.close()
          index.refresh()
          shards -= key
      }
      getMetricFacetShards(metric).foreach {
        case (key, index) =>
          implicit val writer: IndexWriter = index.getWriter
          index.deleteAll()
          writer.close()
          index.refresh()
          facetIndexShards -= key
      }
      sender() ! MetricDropped(db, namespace, metric)
  }

  def readOps: Receive = {
    case GetMetrics(_, _) =>
      sender() ! MetricsGot(db, namespace, shards.keys.map(_.metric).toSet)
    case GetCount(_, ns, metric) =>
      val hits = getMetricShards(metric).map {
        case (_, index) =>
          index.query(new MatchAllDocsQuery(), Seq.empty, Int.MaxValue, None).size
      }.sum
      sender ! CountGot(db, ns, metric, hits)
    case ExecuteSelectStatement(statement, schema) =>
      val combinedResult: Try[Seq[Bit]] = statementParser.parseStatement(statement, schema) match {
        case Success(ParsedSimpleQuery(_, metric, q, false, limit, fields, sort)) =>
          val intervals = TimeRangeExtractor.extractTimeRange(statement.condition.map(_.expression))

          val metricIn: mutable.Map[ShardKey, TimeSeriesIndex] = getMetricShards(statement.metric)
          val indexes = metricIn
            .filter {
              //FIXME see if it can be refactored
              case (key, _) if intervals.nonEmpty =>
                intervals
                  .map(i => Interval.closed(key.from, key.to).intersect(i) != Interval.empty[Long])
                  .foldLeft(false)((x, y) => x || y)
              case _ => true
            }

          val orderedResults = if (statement.getTimeOrdering.isDefined || statement.order.isEmpty) {
            val result: ListBuffer[Try[Seq[Bit]]] = ListBuffer.empty

            val eventuallyOrdered =
              statement.getTimeOrdering.map(indexes.toSeq.sortBy(_._1.from)(_)).getOrElse(indexes.toSeq)

            eventuallyOrdered.takeWhile {
              case (_, index) =>
                val partials = handleQueryResults(metric, Try(index.query(q, fields, limit, sort)))
                result += partials

                val combined = Try(result.flatMap(_.get))

                combined.isSuccess && combined.get.lengthCompare(statement.limit.map(_.value).getOrElse(Int.MaxValue)) < 0
            }

            Try(result.flatMap(_.get))

          } else {

            val shardResults = indexes.toSeq.map {
              case (_, index) =>
                handleQueryResults(metric, Try(index.query(q, fields, limit, sort)))
            }

            Try(shardResults.flatMap(_.get)).map(s => {
              val o = schema.fields.find(_.name == statement.order.get.dimension).get.indexType.ord
              implicit val ord: Ordering[JSerializable] =
                if (statement.order.get.isInstanceOf[DescOrderOperator]) o.reverse else o
              val sorted = s.sortBy(_.dimensions(statement.order.get.dimension))
              sorted.take(statement.limit.get.value)
            })

          }

          if (fields.lengthCompare(1) == 0 && fields.head.count) {
            orderedResults.map(seq => {
              val recordCount = seq.map(_.value.asInstanceOf[Int]).sum
              val count       = if (recordCount <= limit) recordCount else limit
              Seq(Bit(0, count, Map(seq.head.dimensions.head._1 -> count)))
            })
          } else
            orderedResults.map(
              s =>
                s.map(
                  b =>
                    if (b.dimensions.contains("count(*)")) b.copy(dimensions = b.dimensions + ("count(*)" -> s.size))
                    else b)
            )

        case Success(ParsedSimpleQuery(_, metric, q, true, limit, fields, sort)) if fields.lengthCompare(1) == 0 =>
          val distinctField = fields.head.name

          val intervals = TimeRangeExtractor.extractTimeRange(statement.condition.map(_.expression))

          val facetIn = getMetricFacetShards(statement.metric)

          val indexes = facetIn
            .filter {
              case (key, _) if intervals.nonEmpty =>
                intervals
                  .map(i => Interval.closed(key.from, key.to).intersect(i) != Interval.empty[Long])
                  .foldLeft(false)((x, y) => x || y)
              case _ => true
            }

          val results = indexes.toSeq.map {
            case (_, index) =>
              handleQueryResults(metric, Try(index.getDistinctField(q, fields.map(_.name).head, sort, limit)))
          }

          val shardResults = Try(
            results
              .flatMap(_.get)
              .groupBy(_.dimensions(distinctField))
              .mapValues(values => {
                Bit(0, 0, Map[String, JSerializable]((distinctField, values.head.dimensions(distinctField))))
              })
              .values
              .toSet)

          applyOrderingWithLimit(shardResults.map(_.toSeq), statement, schema)

        case Success(ParsedAggregatedQuery(_, metric, q, collector: CountAllGroupsCollector, sort, limit)) =>
          val intervals = TimeRangeExtractor.extractTimeRange(statement.condition.map(_.expression))

          val facetIn = getMetricFacetShards(statement.metric)

          val indexes = facetIn
            .filter {
              case (key, _) if intervals.nonEmpty =>
                intervals
                  .map(i => Interval.closed(key.from, key.to).intersect(i) != Interval.empty[Long])
                  .foldLeft(false)((x, y) => x || y)
              case _ => true
            }

          val result = indexes.toSeq.map {
            case (_, index) =>
              handleQueryResults(metric, Try(index.getCount(q, collector.groupField, sort, limit)))
          }

          val shardResults = Try(
            result
              .flatMap(_.get)
              .groupBy(_.dimensions(statement.groupBy.get))
              .mapValues(values => {
                val v                                        = schema.fields.find(_.name == "value").get.indexType.asInstanceOf[NumericType[_, _]]
                implicit val numeric: Numeric[JSerializable] = v.numeric
                Bit(0, values.map(_.value).sum, values.head.dimensions)
              })
              .values
              .toSeq)

          applyOrderingWithLimit(shardResults, statement, schema)

        case Success(ParsedAggregatedQuery(_, metric, q, collector, sort, limit)) =>
          val indexes = getMetricShards(statement.metric)
          val shardResults = indexes.toSeq.map {
            case (_, index) =>
              handleQueryResults(metric, Try(index.query(q, collector, limit, sort)))
          }
          val rawResult = Try(
            shardResults
              .flatMap(_.get)
              .groupBy(_.dimensions(statement.groupBy.get))
              .mapValues(values => {
                val v                                        = schema.fields.find(_.name == "value").get.indexType.asInstanceOf[NumericType[_, _]]
                implicit val numeric: Numeric[JSerializable] = v.numeric
                collector match {
                  case _: MaxAllGroupsCollector =>
                    Bit(0, values.map(_.value).max, values.head.dimensions)
                  case _: MinAllGroupsCollector =>
                    Bit(0, values.map(_.value).min, values.head.dimensions)
                  case _: SumAllGroupsCollector =>
                    Bit(0, values.map(_.value).sum, values.head.dimensions)
                }
              })
              .values
              .toSeq
          )

          applyOrderingWithLimit(rawResult, statement, schema)

        case Failure(ex) => Failure(ex)
        case _           => Failure(new InvalidStatementException("Not a select statement."))
      }

      combinedResult match {
        case Success(bits) => sender() ! SelectStatementExecuted(db, namespace, statement.metric, bits)
        case Failure(ex)   => sender() ! SelectStatementFailed(ex.getMessage)
      }
  }

  private val opBufferMap: mutable.Map[ShardKey, Seq[Operation]] = mutable.Map.empty

  def accumulate: Receive = {
    case AddRecordToLocation(_, ns, metric, bit, location) =>
      val key = ShardKey(metric, location.from, location.to)
      opBufferMap
        .get(key)
        .fold {
          opBufferMap += (key -> Seq(WriteOperation(ns, metric, bit)))
        } { list =>
          opBufferMap += (key -> (list :+ WriteOperation(ns, metric, bit)))
        }
      sender ! RecordAdded(db, ns, metric, bit)
    case DeleteRecordFromLocation(_, ns, metric, bit, location) =>
      val key = ShardKey(metric, location.from, location.to)
      opBufferMap
        .get(key)
        .fold {
          opBufferMap += (key -> Seq(DeleteRecordOperation(ns, metric, bit)))
        } { list =>
          opBufferMap += (key -> (list :+ DeleteRecordOperation(ns, metric, bit)))
        }
      sender ! RecordDeleted(db, ns, metric, bit)
    case ExecuteDeleteStatementInternal(statement, schema) =>
      statementParser.parseStatement(statement, schema) match {
        case Success(ParsedDeleteQuery(ns, metric, q)) =>
          opBufferMap.filter(_._1.metric == metric).foreach {
            case (key, _) =>
              opBufferMap
                .get(key)
                .fold {
                  opBufferMap += (key -> Seq(DeleteQueryOperation(ns, metric, q)))
                } { list =>
                  opBufferMap += (key -> (list :+ DeleteQueryOperation(ns, metric, q)))
                }
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
      opBufferMap.foreach {
        case (key, ops) =>
          val index                               = getIndex(key)
          val facetIndex                          = getFacetIndex(key)
          implicit val writer: IndexWriter        = index.getWriter
          val facetWriter: IndexWriter            = facetIndex.getWriter
          val taxoWriter: DirectoryTaxonomyWriter = facetIndex.getTaxoWriter
          ops.foreach {
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
              implicit val writer: IndexWriter = index.getWriter
              index.delete(q)
          }
          writer.flush()
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

object ShardActor {

  case object PerformWrites
  case object Accumulate

  def props(basePath: String, db: String, namespace: String): Props = Props(new ShardActor(basePath, db, namespace))
}
