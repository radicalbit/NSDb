/*
 * Copyright 2018 Radicalbit S.r.l.
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

import io.radicalbit.nsdb.common.exception.InvalidStatementException
import io.radicalbit.nsdb.common.statement._
import io.radicalbit.nsdb.index._
import io.radicalbit.nsdb.model.{Schema, SchemaField, TimeRange}
import org.apache.lucene.search._

import scala.util.{Failure, Success, Try}

/**
  * This class exposes method for parsing from a [[io.radicalbit.nsdb.common.statement.SQLStatement]] into a [[io.radicalbit.nsdb.statement.StatementParser.ParsedQuery]].
  */
object StatementParser {

  /**
    * Parses a [[DeleteSQLStatement]] into a [[ParsedQuery]].
    *
    * @param statement the statement to be parsed.
    * @param schema metric'groupFieldType schema.
    * @return a Try of [[ParsedQuery]] to handle errors.
    */
  def parseStatement(statement: DeleteSQLStatement, schema: Schema): Try[ParsedQuery] = {
    val expParsed = ExpressionParser.parseExpression(Some(statement.condition.expression), schema.fieldsMap)
    expParsed.map(exp => ParsedDeleteQuery(statement.namespace, statement.metric, exp.q))
  }

  /**
    * Retrieves internal [[InternalAggregationType]] based on provided into the query.
    * @param groupField group by field.
    * @param aggregateField field to apply the aggregation to.
    * @param agg aggregation clause in query (min, max, sum, count).
    * @return an instance of [[InternalAggregationType]] based on the given parameters.
    */
  private def aggregationType(groupField: String, aggregateField: String, agg: Aggregation): InternalAggregationType = {
    agg match {
      case CountAggregation => InternalCountAggregation(groupField, aggregateField)
      case MaxAggregation   => InternalMaxAggregation(groupField, aggregateField)
      case MinAggregation   => InternalMinAggregation(groupField, aggregateField)
      case SumAggregation   => InternalSumAggregation(groupField, aggregateField)
    }
  }

  /**
    * Parses a [[SelectSQLStatement]] into a [[ParsedQuery]].
    * @param statement the select statement to be parsed.
    * @param schema metric's schema.
    * @return a Try of [[ParsedQuery]] to handle errors.
    */
  def parseStatement(statement: SelectSQLStatement,
                     schema: Schema,
                     occurredOn: Long = System.currentTimeMillis()): Try[ParsedQuery] = {
    val sortOpt = statement.order.map(order => {
      schema.fieldsMap.get(order.dimension) match {
        case Some(SchemaField(_, _, VARCHAR())) =>
          new Sort(new SortField(order.dimension, SortField.Type.STRING, order.isInstanceOf[DescOrderOperator]))
        case Some(SchemaField(_, _, BIGINT())) =>
          new Sort(new SortField(order.dimension, SortField.Type.LONG, order.isInstanceOf[DescOrderOperator]))
        case Some(SchemaField(_, _, INT())) =>
          new Sort(new SortField(order.dimension, SortField.Type.INT, order.isInstanceOf[DescOrderOperator]))
        case Some(SchemaField(_, _, DECIMAL())) =>
          new Sort(new SortField(order.dimension, SortField.Type.DOUBLE, order.isInstanceOf[DescOrderOperator]))
        case _ => new Sort(new SortField(order.dimension, SortField.Type.DOC, order.isInstanceOf[DescOrderOperator]))
      }
    })

    val expParsed = ExpressionParser.parseExpression(statement.condition.map(_.expression), schema.fieldsMap)
    val fieldList = statement.fields match {
      case AllFields => Success(List.empty)
      case ListFields(list) =>
        val metricDimensions     = schema.fields.map(_.name)
        val projectionDimensions = list.map(_.name).filterNot(_ == "*")
        val diff                 = projectionDimensions.filterNot(metricDimensions.contains)
        if (diff.isEmpty)
          Success(list)
        else
          Failure(new InvalidStatementException(StatementParserErrors.notExistingDimensions(diff)))
    }

    val distinctValue = statement.distinct

    val limitOpt = statement.limit.map(_.value)

    expParsed.flatMap(exp =>
      (distinctValue, fieldList, statement.groupBy) match {
        case (_, Failure(exception), _) => Failure(exception)
        // Trying to order by a dimension not in group by clause
        case (false, Success(Seq(Field(_, Some(_)))), Some(group))
            if sortOpt.isDefined && !Seq("value", group.dimension).contains(sortOpt.get.getSort.head.getField) =>
          Failure(new InvalidStatementException(StatementParserErrors.SORT_DIMENSION_NOT_IN_GROUP))
        // Match temporal count aggregation
        case (false, Success(Seq(Field(fieldName, Some(CountAggregation)))), Some(TemporalGroupByAggregation(interval)))
            if fieldName == "value" || fieldName == "*" =>
          val ranges = TimeRangeExtractor.computeRanges(interval, statement.condition, statement.limit, occurredOn)
          Success(
            ParsedTemporalAggregatedQuery(
              statement.namespace,
              statement.metric,
              exp.q,
              ranges,
              sortOpt,
              limitOpt
            )
          )
        // not yet supported aggregations different from count
        case (false, Success(Seq(Field(_, Some(_)))), Some(TemporalGroupByAggregation(_))) =>
          Failure(new InvalidStatementException(StatementParserErrors.NOT_SUPPORTED_AGGREGATION_IN_TEMPORAL_GROUP_BY))
        case (false, Success(Seq(Field(fieldName, Some(agg)))), Some(group: SimpleGroupByAggregation))
            if schema.fields.map(_.name).contains(group.dimension) && (fieldName == "value" || fieldName == "*") =>
          Success(
            ParsedAggregatedQuery(
              statement.namespace,
              statement.metric,
              exp.q,
              aggregationType(groupField = group.dimension, aggregateField = "value", agg = agg), // FIXME: from sql parser aggregation to internal parser aggregation
              sortOpt,
              limitOpt
            ))
        case (false, Success(Seq(Field(_, Some(_)))), Some(group))
            if schema.fields.map(_.name).contains(group.dimension) =>
          Failure(new InvalidStatementException(StatementParserErrors.AGGREGATION_NOT_ON_VALUE))
        case (false, Success(Seq(Field(_, Some(_)))), Some(group)) =>
          Failure(new InvalidStatementException(StatementParserErrors.notExistingDimension(group.dimension)))
        case (_, Success(List(Field(_, None))), Some(_)) =>
          Failure(new InvalidStatementException(StatementParserErrors.NO_AGGREGATION_GROUP_BY))
        case (_, Success(List(_)), Some(_)) =>
          Failure(new InvalidStatementException(StatementParserErrors.MORE_FIELDS_GROUP_BY))
        case (true, Success(List()), None) =>
          Failure(new InvalidStatementException(StatementParserErrors.MORE_FIELDS_DISTINCT))
        //TODO: Not supported yet
        case (true, Success(fieldsSeq), None) if fieldsSeq.lengthCompare(1) > 0 =>
          Failure(new InvalidStatementException(StatementParserErrors.MORE_FIELDS_DISTINCT))
        case (false, Success(Seq(Field(name, Some(CountAggregation)))), None) =>
          Success(
            ParsedSimpleQuery(
              statement.namespace,
              statement.metric,
              exp.q,
              distinct = false,
              limitOpt.getOrElse(Integer.MAX_VALUE),
              List(SimpleField(name, count = true)),
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
                              distinct = false,
                              limitOpt getOrElse Int.MaxValue,
                              List(),
                              sortOpt))

        case (_, Success(fieldsSeq), None)
            if fieldsSeq.exists(f => f.aggregation.isDefined && !(f.aggregation.get == CountAggregation)) =>
          Failure(new InvalidStatementException(StatementParserErrors.NO_GROUP_BY_AGGREGATION))
    })
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
    * @param aggregationType lucene [[InternalAggregationType]] that must be used to collect and aggregate query's results.
    * @param sort lucene [[Sort]] clause. None if no sort has been supplied.
    * @param limit groups limit.
    */
  case class ParsedAggregatedQuery(namespace: String,
                                   metric: String,
                                   q: Query,
                                   aggregationType: InternalAggregationType,
                                   sort: Option[Sort] = None,
                                   limit: Option[Int] = None)
      extends ParsedQuery

  case class ParsedTemporalAggregatedQuery(namespace: String,
                                           metric: String,
                                           q: Query,
                                           ranges: Seq[TimeRange],
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

  sealed trait InternalAggregationType {

    def groupField: String

    def aggregateField: String
  }

  case class InternalCountAggregation(override val groupField: String, override val aggregateField: String)
      extends InternalAggregationType
  case class InternalMaxAggregation(override val groupField: String, override val aggregateField: String)
      extends InternalAggregationType
  case class InternalMinAggregation(override val groupField: String, override val aggregateField: String)
      extends InternalAggregationType
  case class InternalSumAggregation(override val groupField: String, override val aggregateField: String)
      extends InternalAggregationType
}
