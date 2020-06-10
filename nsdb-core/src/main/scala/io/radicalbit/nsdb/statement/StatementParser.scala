/*
 * Copyright 2018-2020 Radicalbit S.r.l.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.radicalbit.nsdb.statement

import io.radicalbit.nsdb.common.statement._
import io.radicalbit.nsdb.model.Schema
import org.apache.lucene.search._

/**
  * This class exposes method for parsing from a [[io.radicalbit.nsdb.common.statement.SQLStatement]] into a [[io.radicalbit.nsdb.statement.StatementParser.ParsedQuery]].
  */
object StatementParser {

  /**
    * Parses a [[DeleteSQLStatement]] into a [[ParsedQuery]].
    *
    * @param statement the statement to be parsed.
    * @param schema    metric'groupFieldType schema.
    * @return a Try of [[ParsedQuery]] to handle errors.
    */
  def parseStatement(statement: DeleteSQLStatement, schema: Schema): Either[String, ParsedQuery] = {
    val expParsed = ExpressionParser.parseExpression(Some(statement.condition.expression), schema.fieldsMap)
    expParsed.map(exp => ParsedDeleteQuery(statement.namespace, statement.metric, exp.q))
  }

  /**
    * Retrieves internal [[InternalSimpleAggregationType]] based on provided into the query.
    *
    * @param groupField     group by field.
    * @param aggregateField field to apply the aggregation to.
    * @param agg            aggregation clause in query (min, max, sum, count).
    * @return an instance of [[InternalSimpleAggregationType]] based on the given parameters.
    */
  private def aggregationType(groupField: String,
                              aggregateField: String,
                              agg: Aggregation): InternalSimpleAggregationType = {
    agg match {
      case CountAggregation => InternalCountSimpleAggregation(groupField, aggregateField)
      case MaxAggregation   => InternalMaxSimpleAggregation(groupField, aggregateField)
      case MinAggregation   => InternalMinSimpleAggregation(groupField, aggregateField)
      case SumAggregation   => InternalSumSimpleAggregation(groupField, aggregateField)
      case FirstAggregation => InternalFirstSimpleAggregation(groupField, aggregateField)
      case LastAggregation  => InternalLastSimpleAggregation(groupField, aggregateField)
    }
  }

  /**
    * Parses a [[SelectSQLStatement]] into a [[ParsedQuery]].
    *
    * @param statement the select statement to be parsed.
    * @param schema    metric's schema.
    * @return a Try of [[ParsedQuery]] to handle errors.
    */
  def parseStatement(statement: SelectSQLStatement, schema: Schema): Either[String, ParsedQuery] = {
    val sortOpt = statement.order.map(order => {
      val sortType = schema.fieldsMap.get(order.dimension).map(_.indexType.sortType).getOrElse(SortField.Type.DOC)
      new Sort(new SortField(order.dimension, sortType, order.isInstanceOf[DescOrderOperator]))
    })

    val expParsed = ExpressionParser.parseExpression(statement.condition.map(_.expression), schema.fieldsMap)
    val fieldList: Either[String, List[Field]] = statement.fields match {
      case AllFields() => Right(List.empty)
      case ListFields(list) =>
        val metricDimensions     = schema.fieldsMap.values.map(_.name).toSeq
        val projectionDimensions = list.map(_.name).filterNot(_ == "*")
        val diff                 = projectionDimensions.filterNot(metricDimensions.contains)
        if (diff.isEmpty)
          Right(list)
        else
          Left(StatementParserErrors.notExistingDimensions(diff))
    }

    val distinctValue = statement.distinct

    val limitOpt = statement.limit.map(_.value)

    expParsed match {
      case Right(exp) =>
        (distinctValue, fieldList, statement.groupBy) match {
          case (_, Left(errorMessage), _) => Left(errorMessage)
          // Trying to order by a dimension not in group by clause
          case (false, Right(Seq(Field(_, Some(_)))), Some(group))
              if sortOpt.isDefined && !Seq("value", group.field).contains(sortOpt.get.getSort.head.getField) =>
            Left(StatementParserErrors.SORT_DIMENSION_NOT_IN_GROUP)
          // Match temporal count aggregation
          case (false,
                Right(Seq(Field(fieldName, Some(aggregation)))),
                Some(TemporalGroupByAggregation(interval, _, _))) if fieldName == "value" || fieldName == "*" =>
            Right(
              ParsedTemporalAggregatedQuery(
                statement.namespace,
                statement.metric,
                exp.q,
                interval,
                InternalTemporalAggregation(aggregation),
                statement.condition,
                sortOpt,
                limitOpt
              )
            )
          case (false, Right(Seq(Field(fieldName, Some(agg)))), Some(group: SimpleGroupByAggregation))
              if schema.tags.contains(group.field) && (fieldName == "value" || fieldName == "*") =>
            Right(
              ParsedAggregatedQuery(
                statement.namespace,
                statement.metric,
                exp.q,
                aggregationType(groupField = group.field, aggregateField = "value", agg = agg),
                sortOpt,
                limitOpt
              ))
          case (false, Right(Seq(Field(fieldName, Some(_)))), Some(_: SimpleGroupByAggregation))
              if fieldName == "value" || fieldName == "*" =>
            Left(StatementParserErrors.SIMPLE_AGGREGATION_NOT_ON_TAG)
          case (false, Right(Seq(Field(_, Some(_)))), Some(group)) if schema.tags.contains(group.field) =>
            Left(StatementParserErrors.AGGREGATION_NOT_ON_VALUE)
          case (false, Right(Seq(Field(_, Some(_)))), Some(group)) =>
            Left(StatementParserErrors.notExistingDimension(group.field))
          case (_, Right(List(Field(_, None))), Some(_)) =>
            Left(StatementParserErrors.NO_AGGREGATION_GROUP_BY)
          case (_, Right(Nil), Some(_)) =>
            Left(StatementParserErrors.NO_AGGREGATION_GROUP_BY)
          case (_, Right(List(_)), Some(_)) =>
            Left(StatementParserErrors.MORE_FIELDS_GROUP_BY)
          case (true, Right(List()), None) =>
            Left(StatementParserErrors.MORE_FIELDS_DISTINCT)
          //TODO: Not supported yet
          case (true, Right(fieldsSeq), None) if fieldsSeq.lengthCompare(1) > 0 =>
            Left(StatementParserErrors.MORE_FIELDS_DISTINCT)
          case (false, Right(Seq(Field(name, Some(CountAggregation)))), None) =>
            Right(
              ParsedSimpleQuery(
                statement.namespace,
                statement.metric,
                exp.q,
                distinct = false,
                limitOpt getOrElse Int.MaxValue,
                List(SimpleField(name, count = true)),
                sortOpt
              )
            )
          case (distinct, Right(fieldsSeq), None)
              if !fieldsSeq.exists(f => f.aggregation.isDefined && f.aggregation.get != CountAggregation) =>
            Right(
              ParsedSimpleQuery(
                statement.namespace,
                statement.metric,
                exp.q,
                distinct,
                limitOpt getOrElse Int.MaxValue,
                fieldsSeq.map(f => SimpleField(f.name, f.aggregation.isDefined)),
                sortOpt
              ))
          case (false, Right(List()), None) =>
            Right(
              ParsedSimpleQuery(statement.namespace,
                                statement.metric,
                                exp.q,
                                distinct = false,
                                limitOpt getOrElse Int.MaxValue,
                                List(),
                                sortOpt))

          case (_, Right(fieldsSeq), None)
              if fieldsSeq.exists(f => f.aggregation.isDefined && !(f.aggregation.get == CountAggregation)) =>
            Left(StatementParserErrors.NO_GROUP_BY_AGGREGATION)
        }
      case Left(errorMessage) => Left(errorMessage)
    }
  }

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
    *
    * @param name  field to be returned into query results.
    * @param count if the number of occurrences of that field must be returned or not. i.e. if the initial query specifies count(field) as a projection.
    */
  case class SimpleField(name: String, count: Boolean = false) {
    override def toString: String = {
      if (count) s"count($name)" else name
    }
  }

  /**
    * Internal query without aggregations.
    *
    * @param namespace query namespace.
    * @param metric    query metric.
    * @param q         lucene's [[Query]]
    * @param distinct  true if results must be distinct, false otherwise.
    * @param limit     results limit.
    * @param fields    subset of fields to be included in the results .
    * @param sort      lucene [[Sort]] clause. None if no sort has been supplied.
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
    *
    * @param namespace       query namespace.
    * @param metric          query metric.
    * @param q               lucene's [[Query]]
    * @param aggregationType lucene [[InternalSimpleAggregationType]] that must be used to collect and aggregate query's results.
    * @param sort            lucene [[Sort]] clause. None if no sort has been supplied.
    * @param limit           groups limit.
    */
  case class ParsedAggregatedQuery(namespace: String,
                                   metric: String,
                                   q: Query,
                                   aggregationType: InternalSimpleAggregationType,
                                   sort: Option[Sort] = None,
                                   limit: Option[Int] = None)
      extends ParsedQuery

  case class ParsedTemporalAggregatedQuery(namespace: String,
                                           metric: String,
                                           q: Query,
                                           rangeLength: Long,
                                           aggregationType: InternalTemporalAggregation,
                                           condition: Option[Condition],
                                           sort: Option[Sort] = None,
                                           limit: Option[Int] = None)
      extends ParsedQuery

  /**
    * Internal query that maps a sql delete statement.
    *
    * @param namespace query namespace.
    * @param metric    query metric.
    * @param q         lucene's [[Query]]
    */
  case class ParsedDeleteQuery(namespace: String, metric: String, q: Query) extends ParsedQuery

  sealed trait InternalAggregation

  sealed trait InternalSimpleAggregationType extends InternalAggregation {

    def groupField: String

    def aggregateField: String
  }

  case class InternalCountSimpleAggregation(override val groupField: String, override val aggregateField: String)
      extends InternalSimpleAggregationType

  case class InternalMaxSimpleAggregation(override val groupField: String, override val aggregateField: String)
      extends InternalSimpleAggregationType

  case class InternalMinSimpleAggregation(override val groupField: String, override val aggregateField: String)
      extends InternalSimpleAggregationType

  case class InternalSumSimpleAggregation(override val groupField: String, override val aggregateField: String)
      extends InternalSimpleAggregationType

  case class InternalFirstSimpleAggregation(override val groupField: String, override val aggregateField: String)
      extends InternalSimpleAggregationType

  case class InternalLastSimpleAggregation(override val groupField: String, override val aggregateField: String)
      extends InternalSimpleAggregationType

  case class InternalAvgSimpleAggregation(override val groupField: String, override val aggregateField: String)
      extends InternalSimpleAggregationType

  sealed trait InternalTemporalAggregation extends InternalAggregation

  object InternalTemporalAggregation {
    def apply(aggregation: Aggregation): InternalTemporalAggregation =
      aggregation match {
        case CountAggregation => InternalCountTemporalAggregation
        case MaxAggregation   => InternalMaxTemporalAggregation
        case MinAggregation   => InternalMinTemporalAggregation
        case SumAggregation   => InternalSumTemporalAggregation
      }
  }

  case object InternalCountTemporalAggregation extends InternalTemporalAggregation

  case object InternalSumTemporalAggregation extends InternalTemporalAggregation

  case object InternalMaxTemporalAggregation extends InternalTemporalAggregation

  case object InternalMinTemporalAggregation extends InternalTemporalAggregation

}
