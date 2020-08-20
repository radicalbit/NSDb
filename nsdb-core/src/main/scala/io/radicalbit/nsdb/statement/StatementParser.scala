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
import io.radicalbit.nsdb.model.{Schema, TimeContext}
import io.radicalbit.nsdb.statement.FieldsParser.SimpleField
import org.apache.lucene.search._

/**
  * This class exposes method for parsing from a [[io.radicalbit.nsdb.common.statement.SQLStatement]] into a [[io.radicalbit.nsdb.statement.StatementParser.ParsedQuery]].
  */
object StatementParser {

  /**
    * Parses a [[DeleteSQLStatement]] into a [[ParsedQuery]].
    *
    * @param statement the statement to be parsed.
    * @param schema    metric groupFieldType schema.
    * @return a Try of [[ParsedQuery]] to handle errors.
    */
  def parseStatement(statement: DeleteSQLStatement, schema: Schema)(
      implicit timeContext: TimeContext): Either[String, ParsedQuery] = {
    val expParsed = ExpressionParser.parseExpression(Some(statement.condition.expression), schema.fieldsMap)
    expParsed.map(exp => ParsedDeleteQuery(statement.namespace, statement.metric, exp.q))
  }

  /**
    * Parses a [[SelectSQLStatement]] into a [[ParsedQuery]].
    *
    * @param statement the select statement to be parsed.
    * @param schema    metric's schema.
    * @return a Try of [[ParsedQuery]] to handle errors.
    */
  def parseStatement(statement: SelectSQLStatement, schema: Schema)(
      implicit timeContext: TimeContext): Either[String, ParsedQuery] = {
    val sortOpt = statement.order.map(order => {
      val sortType = schema.fieldsMap.get(order.dimension).map(_.indexType.sortType).getOrElse(SortField.Type.DOC)
      new Sort(new SortField(order.dimension, sortType, order.isInstanceOf[DescOrderOperator]))
    })

    val expParsed: Either[String, ExpressionParser.ParsedExpression] =
      ExpressionParser.parseExpression(statement.condition.map(_.expression), schema.fieldsMap)

    val distinctValue = statement.distinct

    val limitOpt = statement.limit.map(_.value)

    for {
      exp              <- expParsed
      parsedFieldsList <- FieldsParser.parseFieldList(statement.fields, schema)
      pardedQuery <- {
        (statement.groupBy, parsedFieldsList.list) match {
          case (Some(group), _)
              if sortOpt
                .flatMap(_.getSort.headOption)
                .exists(sort => !Seq("value", "*", group.field).contains(sort.getField)) =>
            Left(StatementParserErrors.SORT_DIMENSION_NOT_IN_GROUP)
          case (Some(_), list) if list.forall(_.aggregation.isEmpty) =>
            Left(StatementParserErrors.NO_AGGREGATION_GROUP_BY)
          case (Some(_), list) if list.size > 1 =>
            Left(StatementParserErrors.MORE_FIELDS_GROUP_BY)
          case (Some(_), _) if distinctValue =>
            Left(StatementParserErrors.GROUP_BY_DISTINCT)
          case (Some(group: SimpleGroupByAggregation), _) if !schema.tags.contains(group.field) =>
            Left(StatementParserErrors.SIMPLE_AGGREGATION_NOT_ON_TAG)
          case (Some(group: SimpleGroupByAggregation), List(SimpleField(_, Some(agg)))) =>
            Right(
              ParsedAggregatedQuery(
                statement.namespace,
                statement.metric,
                exp.q,
                InternalStandardAggregation(group.field, agg),
                sortOpt,
                limitOpt
              ))
          case (Some(TemporalGroupByAggregation(interval, _, _)), List(SimpleField(_, Some(aggregation)))) =>
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
          case (None, fieldsList) if FieldsParser.containsStandardAggregations(fieldsList) =>
            Left(StatementParserErrors.NO_GROUP_BY_AGGREGATION)
          case (None, List()) if distinctValue =>
            Left(StatementParserErrors.MORE_FIELDS_DISTINCT)
          case (None, fieldsList) if distinctValue && fieldsList.size > 1 =>
            Left(StatementParserErrors.MORE_FIELDS_DISTINCT)
          case (None, fieldsList) if FieldsParser.containsOnlyGlobalAggregations(fieldsList) =>
            if (parsedFieldsList.requireTags && schema.isTagless)
              Left(StatementParserErrors.TAGLESS_AGGREGATIONS)
            else {
              val (aggregatedFields, plainFields) = fieldsList.partition(_.aggregation.isDefined)
              Right(
                ParsedGlobalAggregatedQuery(
                  statement.namespace,
                  statement.metric,
                  exp.q,
                  limitOpt.getOrElse(Int.MaxValue),
                  plainFields,
                  aggregatedFields.flatMap(_.aggregation).distinct,
                  sortOpt
                ))
            }
          case (None, fieldsList) =>
            Right(
              ParsedSimpleQuery(
                statement.namespace,
                statement.metric,
                exp.q,
                distinctValue,
                limitOpt getOrElse Int.MaxValue,
                fieldsList,
                sortOpt
              ))
        }
      }
    } yield pardedQuery
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
    * @param namespace        query namespace.
    * @param metric           query metric.
    * @param q                lucene [[Query]]
    * @param limit            results limit.
    * @param plainFields      subset of non aggregated fields to be included in the results.
    * @param aggregations     list of global aggregations to be included in the results.
    * @param sort             lucene [[Sort]] clause. None if no sort has been supplied.
    */
  case class ParsedGlobalAggregatedQuery(namespace: String,
                                         metric: String,
                                         q: Query,
                                         limit: Int,
                                         plainFields: List[SimpleField] = List.empty,
                                         aggregations: List[Aggregation],
                                         sort: Option[Sort] = None)
      extends ParsedQuery

  /**
    * Internal query with group by aggregations
    *
    * @param namespace       query namespace.
    * @param metric          query metric.
    * @param q               lucene's [[Query]]
    * @param aggregation     lucene [[InternalStandardAggregation]] that must be used to collect and aggregate query's results.
    * @param sort            lucene [[Sort]] clause. None if no sort has been supplied.
    * @param limit           groups limit.
    */
  case class ParsedAggregatedQuery(namespace: String,
                                   metric: String,
                                   q: Query,
                                   aggregation: InternalStandardAggregation,
                                   sort: Option[Sort] = None,
                                   limit: Option[Int] = None)
      extends ParsedQuery

  case class ParsedTemporalAggregatedQuery(namespace: String,
                                           metric: String,
                                           q: Query,
                                           rangeLength: Long,
                                           aggregation: InternalTemporalAggregation,
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

  case class InternalStandardAggregation(groupField: String, aggregation: Aggregation) extends InternalAggregation

  case class InternalTemporalAggregation(aggregation: Aggregation) extends InternalAggregation

}
