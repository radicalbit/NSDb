package io.radicalbit.nsdb.statement

import io.radicalbit.nsdb.index.Schema
import io.radicalbit.nsdb.statement.StatementParser.QueryResult
import org.apache.lucene.document.LongPoint
import org.apache.lucene.search._

import scala.util.{Failure, Success, Try}

class StatementParser {

  private def parseExpression(exp: Option[Expression], limit: Int): QueryResult = {
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
        builder.add(parseExpression(Some(expression), limit).q, BooleanClause.Occur.MUST_NOT).build()
      case Some(TupledLogicalExpression(expression1, operator: TupledLogicalOperator, expression2: Expression)) =>
        operator match {
          case AndOperator =>
            val builder = new BooleanQuery.Builder()
            builder.add(parseExpression(Some(expression1), limit).q, BooleanClause.Occur.MUST)
            builder.add(parseExpression(Some(expression2), limit).q, BooleanClause.Occur.MUST).build()
          case OrOperator =>
            val builder = new BooleanQuery.Builder()
            builder.add(parseExpression(Some(expression1), limit).q, BooleanClause.Occur.SHOULD)
            builder.add(parseExpression(Some(expression2), limit).q, BooleanClause.Occur.SHOULD).build()
        }
      case None => new MatchAllDocsQuery()
    }
    QueryResult(q, limit)
  }

  //FIXME this is temporary. In the next PR it will be fixed
  def parseStatement(statement: SelectSQLStatement): Try[QueryResult] = {
    parseStatement(statement, null)
  }

  def parseStatement(statement: SelectSQLStatement, schema: Schema): Try[QueryResult] = {
    statement.limit match {
      case (Some(limit)) =>
        val sortOpt = statement.order.map(order =>
          new Sort(new SortField(order.dimension, SortField.Type.DOC, order.isInstanceOf[DescOrderOperator])))
        val fieldList = statement.fields match {
          case AllFields        => List.empty
          case ListFields(list) => list
        }
        val expParsed = parseExpression(statement.condition.map(_.expression), limit.value)
        Success(expParsed.copy(sort = sortOpt, fields = fieldList))
      case _ => Failure(new RuntimeException("cannot execute query without a limit"))
    }
  }
}

object StatementParser {
  case class QueryResult(q: Query, limit: Int, fields: List[String] = List.empty, sort: Option[Sort] = None)
}
