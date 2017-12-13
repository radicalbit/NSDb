package io.radicalbit.nsdb.statement

import io.radicalbit.nsdb.common.statement._
import io.radicalbit.nsdb.index._
import io.radicalbit.nsdb.index.lucene._
import io.radicalbit.nsdb.model.SchemaField
import io.radicalbit.nsdb.statement.StatementParser._
import org.apache.lucene.document.{DoublePoint, IntPoint, LongPoint}
import org.apache.lucene.index.Term
import org.apache.lucene.search._

import scala.util.{Failure, Success, Try}

class StatementParser {

  private def parseExpression(exp: Option[Expression], schema: Map[String, SchemaField]): Try[ParsedExpression] = {
    val q = exp match {
      case Some(EqualityExpression(dimension, value)) =>
        schema.get(dimension) match {
          case Some(SchemaField(_, t: INT))     => Try(IntPoint.newExactQuery(dimension, t.cast(value)))
          case Some(SchemaField(_, t: BIGINT))  => Try(LongPoint.newExactQuery(dimension, t.cast(value)))
          case Some(SchemaField(_, t: DECIMAL)) => Try(DoublePoint.newExactQuery(dimension, t.cast(value)))
          case Some(SchemaField(_, t: BOOLEAN)) => Try(new TermQuery(new Term(dimension, value.toString)))
          case Some(SchemaField(_, t: CHAR))    => Try(new TermQuery(new Term(dimension, value.toString)))
          case Some(SchemaField(_, t: VARCHAR)) => Try(new TermQuery(new Term(dimension, value.toString)))
          case None                             => Failure(new RuntimeException(s"dimension $dimension not present in metric"))
        }
      case Some(LikeExpression(dimension, value)) =>
        Success(new TermQuery(new Term(dimension, value.replaceAll("\\$", "*"))))
      case Some(ComparisonExpression(dimension, operator: ComparisonOperator, value: Long)) =>
        Success(operator match {
          case GreaterThanOperator      => LongPoint.newRangeQuery(dimension, value + 1, Long.MaxValue)
          case GreaterOrEqualToOperator => LongPoint.newRangeQuery(dimension, value, Long.MaxValue)
          case LessThanOperator         => LongPoint.newRangeQuery(dimension, 0, value - 1)
          case LessOrEqualToOperator    => LongPoint.newRangeQuery(dimension, 0, value)
        })
      case Some(RangeExpression(dimension, v1: Long, v2: Long)) => Success(LongPoint.newRangeQuery(dimension, v1, v2))
      case Some(UnaryLogicalExpression(expression, _)) =>
        parseExpression(Some(expression), schema).map { e =>
          val builder = new BooleanQuery.Builder()
          builder.add(new MatchAllDocsQuery(), BooleanClause.Occur.MUST)
          builder.add(e.q, BooleanClause.Occur.MUST_NOT).build()
        }
      case Some(TupledLogicalExpression(expression1, operator: TupledLogicalOperator, expression2: Expression)) =>
        for {
          e1 <- parseExpression(Some(expression1), schema)
          e2 <- parseExpression(Some(expression2), schema)
        } yield {
          val builder = operator match {
            case AndOperator =>
              val builder = new BooleanQuery.Builder()
              builder.add(e1.q, BooleanClause.Occur.MUST)
              builder.add(e2.q, BooleanClause.Occur.MUST)
            case OrOperator =>
              val builder = new BooleanQuery.Builder()
              builder.add(e1.q, BooleanClause.Occur.SHOULD)
              builder.add(e2.q, BooleanClause.Occur.SHOULD)
          }
          builder.build()
        }
      case None => Success(new MatchAllDocsQuery())
    }
    q.map(ParsedExpression)
  }

  def parseDeleteStatement(statement: DeleteSQLStatement, schema: Schema): Try[ParsedQuery] = {
    parseStatement(statement, schema)
  }

  private def getCollector(group: String, field: String, agg: Aggregation) = {
    agg match {
      case CountAggregation => new CountAllGroupsCollector(group, field)
      case MaxAggregation   => new MaxAllGroupsCollector(group, field)
      case MinAggregation   => new MinAllGroupsCollector(group, field)
      case SumAggregation   => new SumAllGroupsCollector(group, field)
    }
  }

  def parseStatement(statement: DeleteSQLStatement, schema: Schema): Try[ParsedDeleteQuery] = {
    val expParsed = parseExpression(Some(statement.condition.expression), schema.fields.map(f => f.name -> f).toMap)
    expParsed.map(exp => ParsedDeleteQuery(statement.namespace, statement.metric, exp.q))
  }

  def parseStatement(statement: SelectSQLStatement, schema: Schema): Try[ParsedQuery] = {
    val sortOpt = statement.order.map(order => {
      schema.fieldsMap.get(order.dimension) match {
        case Some(SchemaField(_, VARCHAR())) =>
          new Sort(new SortField(order.dimension, SortField.Type.STRING, order.isInstanceOf[DescOrderOperator]))
        case Some(SchemaField(_, BIGINT())) =>
          new Sort(new SortField(order.dimension, SortField.Type.LONG, order.isInstanceOf[DescOrderOperator]))
        case Some(SchemaField(_, INT())) =>
          new Sort(new SortField(order.dimension, SortField.Type.INT, order.isInstanceOf[DescOrderOperator]))
        case Some(SchemaField(_, DECIMAL())) =>
          new Sort(new SortField(order.dimension, SortField.Type.DOUBLE, order.isInstanceOf[DescOrderOperator]))
        case _ => new Sort(new SortField(order.dimension, SortField.Type.DOC, order.isInstanceOf[DescOrderOperator]))
      }
    })
    val expParsed = parseExpression(statement.condition.map(_.expression), schema.fieldsMap)
    val fieldList = statement.fields match {
      case AllFields        => List.empty
      case ListFields(list) => list
    }

    expParsed.flatMap(exp =>
      (fieldList, statement.groupBy, statement.limit) match {
        case (Seq(Field(fieldName, Some(agg))), Some(group), limit) =>
          Success(
            ParsedAggregatedQuery(statement.namespace,
                                  statement.metric,
                                  exp.q,
                                  getCollector(group, fieldName, agg),
                                  sortOpt,
                                  limit.map(_.value)))
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
                              exp.q,
                              limit.value,
                              fieldsSeq.map(_.name),
                              sortOpt))
        case (List(), None, Some(limit)) =>
          Success(ParsedSimpleQuery(statement.namespace, statement.metric, exp.q, limit.value, List(), sortOpt))
        case (fieldsSeq, None, Some(_)) if fieldsSeq.map(_.aggregation.isDefined).foldLeft(true)(_ && _) =>
          Failure(new RuntimeException("cannot execute a query with aggregation without a group by"))
        case (_, None, None) =>
          Failure(new RuntimeException("cannot execute query without a limit"))
    })
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
                                   collector: AllGroupsAggregationCollector,
                                   sort: Option[Sort] = None,
                                   limit: Option[Int] = None)
      extends ParsedQuery

  case class ParsedDeleteQuery(namespace: String, metric: String, q: Query) extends ParsedQuery
}
