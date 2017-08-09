package io.radicalbit.nsdb.index

import java.nio.file.Paths

import akka.actor.{Actor, Props}
import io.radicalbit.index.BoundedIndex
import io.radicalbit.nsdb.coordinator.ReadCoordinator
import io.radicalbit.nsdb.model.Record
import io.radicalbit.nsdb.statement._
import org.apache.lucene.document.LongPoint
import org.apache.lucene.search.{BooleanClause, BooleanQuery, Query}
import org.apache.lucene.store.FSDirectory

import scala.util.{Failure, Success, Try}

class IndexerActor(basePath: String) extends Actor {
  import io.radicalbit.nsdb.index.IndexerActor._

  import scala.collection.mutable

  private val indexes: mutable.Map[String, BoundedIndex] = mutable.Map.empty

  private def getIndex(metric: String) =
    indexes.getOrElse(metric, {
      val path     = FSDirectory.open(Paths.get(basePath, metric))
      val newIndex = new BoundedIndex(path)
      indexes + (metric -> newIndex)
      newIndex
    })

  private def parseExpression(exp: Expression): Query = {
    exp match {
      case ComparisonExpression(dimension, operator: ComparisonOperator, value: Long) => {
        operator match {
          case GreaterThanOperator      => LongPoint.newRangeQuery(dimension, value + 1, Long.MaxValue)
          case GreaterOrEqualToOperator => LongPoint.newRangeQuery(dimension, value, Long.MaxValue)
          case LessThanOperator         => LongPoint.newRangeQuery(dimension, 0, value - 1)
          case LessOrEqualToOperator    => LongPoint.newRangeQuery(dimension, 0, value)
        }
      }
      case RangeExpression(dimension, v1: Long, v2: Long) => LongPoint.newRangeQuery(dimension, v1, v2)
      case UnaryLogicalExpression(expression, _) =>
        val builder = new BooleanQuery.Builder()
        builder.add(parseExpression(expression), BooleanClause.Occur.MUST_NOT).build()
      case TupledLogicalExpression(expression1, operator: TupledLogicalOperator, expression2: Expression) =>
        operator match {
          case AndOperator =>
            val builder = new BooleanQuery.Builder()
            builder.add(parseExpression(expression1), BooleanClause.Occur.MUST)
            builder.add(parseExpression(expression2), BooleanClause.Occur.MUST).build()
          case OrOperator =>
            val builder = new BooleanQuery.Builder()
            builder.add(parseExpression(expression1), BooleanClause.Occur.SHOULD)
            builder.add(parseExpression(expression2), BooleanClause.Occur.SHOULD).build()
        }
    }
  }

  private def parseStatement(statament: SelectSQLStatement): Try[Query] = {

    (statament.limit, statament.condition) match {
      case (Some(limit), Some(condition)) =>
        Success(parseExpression(condition.expression))
      case _ => Failure(new RuntimeException("cannot execute query withour a limit or a condition"))
    }

  }

  override def receive: Receive = {
    case AddRecord(metric, record) =>
      val index           = getIndex(metric)
      implicit val writer = index.getWriter
      index.write(record)
      writer.flush()
      writer.close()
      sender ! RecordAdded(metric, record)
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
    case ReadCoordinator.ExecuteSelectStatement(statement) => {
      val query = parseStatement(statement).get
      getIndex(statement.metric).query(query, statement.limit.get.value, None)
    }
  }
}

object IndexerActor {

  def props(basePath: String): Props = Props(new IndexerActor(basePath))

  case class AddRecord(metric: String, record: Record)
  case class DeleteRecord(metric: String, record: Record)
  case class DeleteMetric(metric: String)
  case class GetCount(metric: String)
  case class CountGot(metric: String, count: Int)
  case class RecordAdded(metric: String, record: Record)
  case class RecordRejected(metric: String, record: Record, reason: String)
  case class RecordDeleted(metric: String, record: Record)
  case class MetricDeleted(metric: String)
}
