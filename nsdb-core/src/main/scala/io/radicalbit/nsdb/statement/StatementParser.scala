package io.radicalbit.nsdb.statement

import io.radicalbit.nsdb.common.JSerializable
import io.radicalbit.nsdb.common.exception.InvalidStatementException
import io.radicalbit.nsdb.common.statement._
import io.radicalbit.nsdb.index._
import io.radicalbit.nsdb.index.lucene._
import io.radicalbit.nsdb.model.{Schema, SchemaField}
import io.radicalbit.nsdb.statement.StatementParser._
import org.apache.lucene.document.{DoublePoint, IntPoint, LongPoint}
import org.apache.lucene.index.Term
import org.apache.lucene.search._

import scala.reflect.ClassTag
import scala.util.{Failure, Success, Try}

/**
  * This class exposes method for parsing from a [[SQLStatement]] into a [[ParsedQuery]].
  */
class StatementParser {

  /**
    * Parses an optional [[Expression]] into a [[ParsedExpression]].
    * @param exp the expression to be parsed.
    * @param schema metric'groupFieldType schema fields map.
    * @return a Try of [[ParsedExpression]] to potential errors.
    */
  private def parseExpression(exp: Option[Expression], schema: Map[String, SchemaField]): Try[ParsedExpression] = {
    val q = exp match {
      case Some(NullableExpression(dimension)) =>
        val query = schema.get(dimension) match {
          case Some(SchemaField(_, _: INT)) =>
            Try(IntPoint.newRangeQuery(dimension, Int.MinValue, Int.MaxValue))
          case Some(SchemaField(_, _: BIGINT)) =>
            Try(LongPoint.newRangeQuery(dimension, Long.MinValue, Long.MaxValue))
          case Some(SchemaField(_, _: DECIMAL)) =>
            Try(DoublePoint.newRangeQuery(dimension, Double.MinValue, Double.MaxValue))
          case Some(SchemaField(_, _: VARCHAR)) => Try(new WildcardQuery(new Term(dimension, "*")))
          case None                             => Failure(new InvalidStatementException(Errors.notExistingDimension(dimension)))
        }
        // Used to apply negation due to the fact Lucene does not support nullable fields, query the values' range and apply negation
        query.map { qq =>
          val builder = new BooleanQuery.Builder()
          builder.add(new MatchAllDocsQuery(), BooleanClause.Occur.MUST)
          builder.add(qq, BooleanClause.Occur.MUST_NOT).build()
        }
      case Some(EqualityExpression(dimension, value)) =>
        schema.get(dimension) match {
          case Some(SchemaField(_, t: INT)) =>
            Try(IntPoint.newExactQuery(dimension, t.cast(value))) recoverWith {
              case _ => Failure(new InvalidStatementException(Errors.uncompatibleOperator("equality", "INT")))
            }
          case Some(SchemaField(_, t: BIGINT)) =>
            Try(LongPoint.newExactQuery(dimension, t.cast(value))) recoverWith {
              case _ => Failure(new InvalidStatementException(Errors.uncompatibleOperator("equality", "BIGINT")))
            }
          case Some(SchemaField(_, t: DECIMAL)) =>
            Try(DoublePoint.newExactQuery(dimension, t.cast(value))) recoverWith {
              case _ => Failure(new InvalidStatementException(Errors.uncompatibleOperator("equality", "DECIMAL")))
            }
          case Some(SchemaField(_, _: VARCHAR)) => Try(new TermQuery(new Term(dimension, value.toString)))
          case None                             => Failure(new InvalidStatementException(Errors.notExistingDimension(dimension)))
        }
      case Some(LikeExpression(dimension, value)) =>
        schema.get(dimension) match {
          case Some(SchemaField(_, _: VARCHAR)) =>
            Success(new WildcardQuery(new Term(dimension, value.replaceAll("\\$", "*"))))
          case Some(_) =>
            Failure(new InvalidStatementException(Errors.uncompatibleOperator("Like", "VARCHAR")))
          case None => Failure(new InvalidStatementException(Errors.notExistingDimension(dimension)))
        }
      case Some(ComparisonExpression(dimension, operator: ComparisonOperator, value)) =>
        def buildRangeQuery[T](fieldTypeRangeQuery: (String, T, T) => Query,
                               greaterF: T,
                               lessThan: T,
                               min: T,
                               max: T,
                               v: T): Try[Query] =
          Success(operator match {
            case GreaterThanOperator      => fieldTypeRangeQuery(dimension, greaterF, max)
            case GreaterOrEqualToOperator => fieldTypeRangeQuery(dimension, v, max)
            case LessThanOperator         => fieldTypeRangeQuery(dimension, min, lessThan)
            case LessOrEqualToOperator    => fieldTypeRangeQuery(dimension, min, v)
          })

        (schema.get(dimension), value) match {
          case (Some(SchemaField(_, t: INT)), v) =>
            Try(t.cast(v)).flatMap(v =>
              buildRangeQuery[Int](IntPoint.newRangeQuery, v + 1, v - 1, Int.MinValue, Int.MaxValue, v)) recoverWith {
              case _ => Failure(new InvalidStatementException(Errors.uncompatibleOperator("range", "INT")))
            }
          case (Some(SchemaField(_, t: BIGINT)), v) =>
            Try(t.cast(v)).flatMap(v =>
              buildRangeQuery[Long](LongPoint.newRangeQuery, v + 1, v - 1, Long.MinValue, Long.MaxValue, v)) recoverWith {
              case _ => Failure(new InvalidStatementException(Errors.uncompatibleOperator("range", "BIGINT")))
            }
          case (Some(SchemaField(_, t: DECIMAL)), v) =>
            Try(t.cast(v)).flatMap(
              v =>
                buildRangeQuery[Double](DoublePoint.newRangeQuery,
                                        Math.nextAfter(v, Double.MaxValue),
                                        Math.nextAfter(v, Double.MinValue),
                                        Double.MinValue,
                                        Double.MaxValue,
                                        v)) recoverWith {
              case _ => Failure(new InvalidStatementException(Errors.uncompatibleOperator("range", "DECIMAL")))
            }
          case (Some(_), _) =>
            Failure(new InvalidStatementException(Errors.uncompatibleOperator("comparison", "numerical")))
          case (None, _) => Failure(new InvalidStatementException(Errors.notExistingDimension(dimension)))
        }
      case Some(RangeExpression(dimension, p1, p2)) =>
        (schema.get(dimension), p1, p2) match {
          case (Some(SchemaField(_, t: BIGINT)), v1, v2) =>
            Try((t.cast(v1), t.cast(v2))).map {
              case (l, h) => LongPoint.newRangeQuery(dimension, l, h)
            } recoverWith {
              case _ => Failure(new InvalidStatementException(Errors.uncompatibleOperator("range", "BIGINT")))
            }
          case (Some(SchemaField(_, t: INT)), v1, v2) =>
            Try((t.cast(v1), t.cast(v2))).map {
              case (l, h) => IntPoint.newRangeQuery(dimension, l, h)
            } recoverWith {
              case _ => Failure(new InvalidStatementException(Errors.uncompatibleOperator("range", "INT")))
            }
          case (Some(SchemaField(_, t: DECIMAL)), v1, v2) =>
            Try((t.cast(v1), t.cast(v2))).map {
              case (l, h) => DoublePoint.newRangeQuery(dimension, l, h)
            } recoverWith {
              case _ => Failure(new InvalidStatementException(Errors.uncompatibleOperator("range", "DECIMAL")))
            }
          case (Some(SchemaField(_, _: VARCHAR)), _, _) =>
            Failure(new InvalidStatementException(Errors.uncompatibleOperator("range", "numerical")))
          case (None, _, _) => Failure(new InvalidStatementException(Errors.notExistingDimension(dimension)))
        }

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

  /**
    * Parses a [[DeleteSQLStatement]] into a [[ParsedQuery]].
    * @param statement the statement to be parsed.
    * @param schema metric'groupFieldType schema.
    * @return a Try of [[ParsedQuery]] to handle errors.
    */
  def parseStatement(statement: DeleteSQLStatement, schema: Schema): Try[ParsedQuery] = {
    val expParsed = parseExpression(Some(statement.condition.expression), schema.fieldsMap)
    expParsed.map(exp => ParsedDeleteQuery(statement.namespace, statement.metric, exp.q))
  }

  /**
    * Retrieves internal [[AllGroupsAggregationCollector]] based on [[AllGroupsAggregationCollector]] provided into the query.
    * @param groupField group by field.
    * @param aggregateField filed to apply the aggregation to.
    * @param agg aggregation clause in query (min, max, sum, count).
    * @param aggregateFieldType aggregate field internal type descriptor, as a subclass of [[NumericType]].
    * @param groupFieldType group field internal type descriptor, as a subclass of [[IndexType]].
    * @return an instance of [[AllGroupsAggregationCollector]] based on the given parameters.
    */
  private def getCollector[T <: NumericType[_, _], S <: IndexType[_]](
      groupField: String,
      aggregateField: String,
      agg: Aggregation,
      aggregateFieldType: T,
      groupFieldType: S): AllGroupsAggregationCollector[_, _] = {
    agg match {
      case CountAggregation =>
        new CountAllGroupsCollector(groupField, aggregateField)(groupFieldType.ord,
                                                                ClassTag(groupFieldType.actualType))
      case MaxAggregation =>
        new MaxAllGroupsCollector(groupField, aggregateField)(aggregateFieldType.scalaNumeric,
                                                              groupFieldType.ord,
                                                              ClassTag(groupFieldType.actualType))
      case MinAggregation =>
        new MinAllGroupsCollector(groupField, aggregateField)(aggregateFieldType.scalaNumeric,
                                                              groupFieldType.ord,
                                                              ClassTag(groupFieldType.actualType))
      case SumAggregation =>
        new SumAllGroupsCollector(groupField, aggregateField)(aggregateFieldType.scalaNumeric,
                                                              groupFieldType.ord,
                                                              ClassTag(groupFieldType.actualType))
    }
  }

  /**
    * Parses a [[SelectSQLStatement]] into a [[ParsedQuery]].
    * @param statement the select statement to be parsed.
    * @param schema metric's schema.
    * @return a Try of [[ParsedQuery]] to handle errors.
    */
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
      case AllFields => Success(List.empty)
      case ListFields(list) =>
        val metricDimensions     = schema.fields.map(_.name)
        val projectionDimensions = list.map(_.name).filterNot(_ == "*")
        val diff                 = projectionDimensions.filterNot(metricDimensions.contains)
        if (diff.isEmpty)
          Success(list)
        else
          Failure(new InvalidStatementException(Errors.notExistingDimensions(diff)))
    }

    val distinctValue = statement.distinct

    val limitOpt = statement.limit.map(_.value)

    expParsed.flatMap(exp =>
      (distinctValue, fieldList, statement.groupBy) match {
        case (_, Failure(exception), _) => Failure(exception)
        case (false, Success(Seq(Field(_, Some(_)))), Some(group))
            if sortOpt.isDefined && !Seq("value", group).contains(sortOpt.get.getSort.head.getField) =>
          Failure(new InvalidStatementException(Errors.SORT_DIMENSION_NOT_IN_GROUP))
        case (false, Success(Seq(Field(fieldName, Some(agg)))), Some(group))
            if schema.fields.map(_.name).contains(group) && (fieldName == "value" || fieldName == "*") =>
          Success(
            ParsedAggregatedQuery(
              statement.namespace,
              statement.metric,
              exp.q,
              getCollector(group,
                           "value",
                           agg,
                           schema.fieldsMap("value").indexType.asInstanceOf[NumericType[_ <: JSerializable, _]],
                           schema.fieldsMap(group).indexType),
              sortOpt,
              limitOpt
            ))
        case (false, Success(Seq(Field(fieldName, Some(_)))), Some(group))
            if schema.fields.map(_.name).contains(group) =>
          Failure(new InvalidStatementException(Errors.AGGREGATION_NOT_ON_VALUE))
        case (false, Success(Seq(Field(_, Some(_)))), Some(group)) =>
          Failure(new InvalidStatementException(Errors.notExistingDimension(group)))
        case (_, Success(List(Field(_, None))), Some(_)) =>
          Failure(new InvalidStatementException(Errors.NO_AGGREGATION_GROUP_BY))
        case (_, Success(List(_)), Some(_)) =>
          Failure(new InvalidStatementException(Errors.MORE_FIELDS_GROUP_BY))
        case (true, Success(List()), None) =>
          Failure(new InvalidStatementException(Errors.MORE_FIELDS_DISTINCT))
        //TODO: Not supported yet
        case (true, Success(fieldsSeq), None) if fieldsSeq.lengthCompare(1) > 0 =>
          Failure(new InvalidStatementException(Errors.MORE_FIELDS_DISTINCT))
        case (false, Success(Seq(Field(name, Some(CountAggregation)))), None) =>
          Success(
            ParsedSimpleQuery(
              statement.namespace,
              statement.metric,
              exp.q,
              false,
              limitOpt.getOrElse(Integer.MAX_VALUE),
              List(SimpleField(name, true)),
              sortOpt
            )
          )
        case (distinct, Success(fieldsSeq), None)
            if !fieldsSeq.exists(f => f.aggregation.isDefined && f.aggregation.get != CountAggregation) =>
          Success(
            ParsedSimpleQuery(
              statement.namespace,
              statement.metric,
              exp.q,
              distinct,
              limitOpt getOrElse Int.MaxValue,
              fieldsSeq.map(f => SimpleField(f.name, f.aggregation.isDefined)),
              sortOpt
            ))
        case (false, Success(List()), None) =>
          Success(
            ParsedSimpleQuery(statement.namespace,
                              statement.metric,
                              exp.q,
                              false,
                              limitOpt getOrElse Int.MaxValue,
                              List(),
                              sortOpt))

        case (_, Success(fieldsSeq), None)
            if fieldsSeq.exists(f => f.aggregation.isDefined && !(f.aggregation.get == CountAggregation)) =>
          Failure(new InvalidStatementException(Errors.NO_GROUP_BY_AGGREGATION))
    })
  }
}

object StatementParser {

  object Errors {
    lazy val NO_AGGREGATION_GROUP_BY = "cannot execute a groupField by query without an aggregation"
    lazy val MORE_FIELDS_GROUP_BY    = "cannot execute a groupField by query with more than a aggregateField"
    lazy val MORE_FIELDS_DISTINCT    = "cannot execute a select distinct projecting more than one dimension"
    lazy val NO_GROUP_BY_AGGREGATION =
      "cannot execute a query with aggregation different than count without a groupField by"
    lazy val AGGREGATION_NOT_ON_VALUE =
      "cannot execute a groupField by query performing an aggregation on dimension different from value"
    lazy val SORT_DIMENSION_NOT_IN_GROUP =
      "cannot sort groupField by query result by a dimension not in groupField"
    def notExistingDimension(dim: String)       = s"dimension $dim does not exist"
    def notExistingDimensions(dim: Seq[String]) = s"dimensions [$dim] does not exist"
    def uncompatibleOperator(operator: String, dimTypeAllowed: String) =
      s"cannot use $operator operator on dimension different from $dimTypeAllowed"
  }

  private case class ParsedExpression(q: Query)

  /**
    * Query to be used directly against lucene indexes. It contains the internal [[Query]] as well as the namespace and metric info.
    */
  sealed trait ParsedQuery {
    val namespace: String
    val metric: String
    val q: Query
  }

  /**
    * Simple query field.
    * @param name field to be returned into query results.
    * @param count if the number of occurrences of that field must be returned or not. i.e. if the initial query specifies count(field) as a projection.
    */
  case class SimpleField(name: String, count: Boolean = false) {
    override def toString: String = {
      if (count) s"count($name)" else name
    }
  }

  /**
    * Internal query without aggregations.
    * @param namespace query namespace.
    * @param metric query metric.
    * @param q lucene's [[Query]]
    * @param distinct true if results must be distinct, false otherwise.
    * @param limit results limit.
    * @param fields subset of fields to be included in the results .
    * @param sort lucene [[Sort]] clause. None if no sort has been supplied.
    */
  case class ParsedSimpleQuery(namespace: String,
                               metric: String,
                               q: Query,
                               distinct: Boolean,
                               limit: Int,
                               fields: List[SimpleField] = List.empty,
                               sort: Option[Sort] = None)
      extends ParsedQuery

  /**
    * Internal query with aggregations
    * @param namespace query namespace.
    * @param metric query metric.
    * @param q lucene's [[Query]]
    * @param collector lucene [[AllGroupsAggregationCollector]] that must be used to collect and aggregate query's results.
    * @param sort lucene [[Sort]] clause. None if no sort has been supplied.
    * @param limit groups limit.
    */
  case class ParsedAggregatedQuery(namespace: String,
                                   metric: String,
                                   q: Query,
                                   collector: AllGroupsAggregationCollector[_, _],
                                   sort: Option[Sort] = None,
                                   limit: Option[Int] = None)
      extends ParsedQuery

  /**
    * Internal query that maps a sql delete statement.
    * @param namespace query namespace.
    * @param metric query metric.
    * @param q lucene's [[Query]]
    */
  case class ParsedDeleteQuery(namespace: String, metric: String, q: Query) extends ParsedQuery
}
