package io.radicalbit.nsdb.statement

import io.radicalbit.nsdb.common.statement._
import io.radicalbit.nsdb.index.Schema
import io.radicalbit.nsdb.index.lucene._
import io.radicalbit.nsdb.statement.StatementParser.{
  ParsedAggregatedQuery,
  ParsedExpression,
  ParsedQuery,
  ParsedSimpleQuery
}
import org.apache.lucene.document.LongPoint
import org.apache.lucene.search._

import scala.util.{Failure, Success, Try}

class StatementParser {

  private def parseExpression(exp: Option[Expression]): ParsedExpression = {
    val q = exp match {
      case Some(ComparisonExpression(dimension, operator: ComparisonOperator, value: Long)) =>
        operator match {
          case GreaterThanOperator      => LongPoint.newRangeQuery(dimension, value + 1, Long.MaxValue)
          case GreaterOrEqualToOperator => LongPoint.newRangeQuery(dimension, value, Long.MaxValue)
          case LessThanOperator         => LongPoint.newRangeQuery(dimension, 0, value - 1)
          case LessOrEqualToOperator    => LongPoint.newRangeQuery(dimension, 0, value)
        }
      case Some(RangeExpression(dimension, v1: Long, v2: Long)) => LongPoint.newRangeQuery(dimension, v1, v2)
      case Some(UnaryLogicalExpression(expression, _)) =>
        val builder = new BooleanQuery.Builder()
        builder.add(parseExpression(Some(expression)).q, BooleanClause.Occur.MUST_NOT).build()
      case Some(TupledLogicalExpression(expression1, operator: TupledLogicalOperator, expression2: Expression)) =>
        operator match {
          case AndOperator =>
            val builder = new BooleanQuery.Builder()
            builder.add(parseExpression(Some(expression1)).q, BooleanClause.Occur.MUST)
            builder.add(parseExpression(Some(expression2)).q, BooleanClause.Occur.MUST).build()
          case OrOperator =>
            val builder = new BooleanQuery.Builder()
            builder.add(parseExpression(Some(expression1)).q, BooleanClause.Occur.SHOULD)
            builder.add(parseExpression(Some(expression2)).q, BooleanClause.Occur.SHOULD).build()
        }
      case None => new MatchAllDocsQuery()
    }
    ParsedExpression(q)
  }

  def parseStatement(statement: SelectSQLStatement): Try[ParsedQuery] = {
    parseStatement(statement, null)
  }

  private def getCollector(group: String, field: String, agg: Aggregation) = {
    agg match {
      case CountAggregation => new CountAllGroupsCollector(group, field)
      case MaxAggregation   => new MaxAllGroupsCollector(group, field)
      case MinAggregation   => new MinAllGroupsCollector(group, field)
      case SumAggregation   => new SumAllGroupsCollector(group, field)
    }
  }

  def parseStatement(statement: SelectSQLStatement, schema: Schema): Try[ParsedQuery] = {
    val sortOpt = statement.order.map(order =>
      new Sort(new SortField(order.dimension, SortField.Type.DOC, order.isInstanceOf[DescOrderOperator])))
    val expParsed = parseExpression(statement.condition.map(_.expression))
    val fieldList = statement.fields match {
      case AllFields        => List.empty
      case ListFields(list) => list
    }
    (fieldList, statement.groupBy, statement.limit) match {
      case (Seq(Field(fieldName, Some(agg))), Some(group), _) =>
        Success(
          ParsedAggregatedQuery(statement.namespace,
                                statement.metric,
                                expParsed.q,
                                getCollector(group, fieldName, agg)))
      case (List(Field(_, None)), Some(_), _) =>
        Failure(new RuntimeException("cannot execute a group by query without an aggregation"))
      case (List(_), Some(_), _) =>
        Failure(new RuntimeException("cannot execute a group by query with more than a field"))
      case (List(), Some(_), _) =>
        Failure(new RuntimeException("cannot execute a group by query with all fields selected"))
      case (fieldsSeq, None, Some(limit)) if fieldsSeq.map(_.aggregation.isEmpty).foldLeft(true)(_ && _) =>
        Success(
          ParsedSimpleQuery(statement.namespace,
                            statement.metric,
                            expParsed.q,
                            limit.value,
                            fieldsSeq.map(_.name),
                            sortOpt))
      case (List(), None, Some(limit)) =>
        Success(ParsedSimpleQuery(statement.namespace, statement.metric, expParsed.q, limit.value, List(), sortOpt))
      case (fieldsSeq, None, Some(_)) if fieldsSeq.map(_.aggregation.isDefined).foldLeft(true)(_ && _) =>
        Failure(new RuntimeException("cannot execute a query with aggregation without a group by"))
      case (_, None, None) =>
        Failure(new RuntimeException("cannot execute query without a limit"))
    }
  }
}

object StatementParser {

  private case class ParsedExpression(q: Query)

  sealed trait ParsedQuery {
    val namespace: String
    val metric: String
    val q: Query
  }
  case class ParsedSimpleQuery(namespace: String,
                               metric: String,
                               q: Query,
                               limit: Int,
                               fields: List[String] = List.empty,
                               sort: Option[Sort] = None)
      extends ParsedQuery
  case class ParsedAggregatedQuery(namespace: String,
                                   metric: String,
                                   q: Query,
                                   collector: AllGroupsAggregationCollector)
      extends ParsedQuery
}
