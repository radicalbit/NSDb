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

import io.radicalbit.nsdb.common.NSDbType
import io.radicalbit.nsdb.common.statement._
import io.radicalbit.nsdb.index.{BIGINT, DECIMAL, INT, VARCHAR}
import io.radicalbit.nsdb.model.{SchemaField, TimeContext}
import org.apache.lucene.document.{DoublePoint, IntPoint, LongPoint}
import org.apache.lucene.index.Term
import org.apache.lucene.search._

import scala.util.{Success, Try}

object ExpressionParser {

  case class ParsedExpression(q: Query)

  /**
    * Parses an optional [[Expression]] into a [[ParsedExpression]].
    * @param exp the expression to be parsed.
    * @param schema metric'groupFieldType schema fields map.
    * @return a Try of [[ParsedExpression]] to potential errors.
    */
  def parseExpression(exp: Option[Expression], schema: Map[String, SchemaField])(
      implicit timeContext: TimeContext): Either[String, ParsedExpression] = {
    val q = exp match {
      case Some(NullableExpression(dimension)) => nullableExpression(schema, dimension)

      case Some(EqualityExpression(dimension, comparison)) =>
        equalityExpression(schema, dimension, comparison.absoluteValue(timeContext.currentTime))
      case Some(LikeExpression(dimension, value)) => likeExpression(schema, dimension, value)
      case Some(ComparisonExpression(dimension, operator: ComparisonOperator, comparison)) =>
        comparisonExpression(schema, dimension, operator, comparison.absoluteValue(timeContext.currentTime))
      case Some(RangeExpression(dimension, comparison1, comparison2)) =>
        rangeExpression(schema,
                        dimension,
                        comparison1.absoluteValue(timeContext.currentTime),
                        comparison2.absoluteValue(timeContext.currentTime))

      case Some(NotExpression(expression, _)) => unaryLogicalExpression(schema, expression)

      case Some(TupledLogicalExpression(expression1, operator: TupledLogicalOperator, expression2: Expression)) =>
        tupledLogicalExpression(schema, expression1, operator, expression2)

      case None => Right(new MatchAllDocsQuery())
    }

    q.map(ParsedExpression)
  }

  private def nullableExpression(schema: Map[String, SchemaField], field: String): Either[String, BooleanQuery] = {
    val query = schema.get(field) match {
      case Some(SchemaField(_, _, INT())) =>
        Right(IntPoint.newRangeQuery(field, Int.MinValue, Int.MaxValue))
      case Some(SchemaField(_, _, BIGINT())) =>
        Right(LongPoint.newRangeQuery(field, Long.MinValue, Long.MaxValue))
      case Some(SchemaField(_, _, DECIMAL())) =>
        Right(DoublePoint.newRangeQuery(field, Double.MinValue, Double.MaxValue))
      case Some(SchemaField(_, _, VARCHAR())) => Right(new WildcardQuery(new Term(field, "*")))
      case None                               => Left(StatementParserErrors.notExistingField(field))
    }
    // Used to apply negation due to the fact Lucene does not support nullable fields, query the values' range and apply negation
    query.map { qq =>
      val builder = new BooleanQuery.Builder()
      builder.add(new MatchAllDocsQuery(), BooleanClause.Occur.MUST)
      builder.add(qq, BooleanClause.Occur.MUST_NOT).build()
    }
  }

  private def equalityExpression(schema: Map[String, SchemaField],
                                 field: String,
                                 value: NSDbType): Either[String, Query] = {
    schema.get(field) match {
      case Some(SchemaField(_, _, t: INT)) =>
        Try(IntPoint.newExactQuery(field, t.cast(value.rawValue))) match {
          case Success(q) => Right(q)
          case _ =>
            Left(StatementParserErrors.nonCompatibleOperator("equality", "INT"))
        }
      case Some(SchemaField(_, _, t: BIGINT)) =>
        Try(LongPoint.newExactQuery(field, t.cast(value.rawValue))) match {
          case Success(q) => Right(q)
          case _          => Left(StatementParserErrors.nonCompatibleOperator("equality", "BIGINT"))
        }
      case Some(SchemaField(_, _, t: DECIMAL)) =>
        Try(DoublePoint.newExactQuery(field, t.cast(value.rawValue))) match {
          case Success(q) => Right(q)
          case _          => Left(StatementParserErrors.nonCompatibleOperator("equality", "DECIMAL"))
        }
      case Some(SchemaField(_, _, _: VARCHAR)) => Right(new TermQuery(new Term(field, value.rawValue.toString)))
      case None                                => Left(StatementParserErrors.notExistingField(field))
    }
  }

  private def likeExpression(schema: Map[String, SchemaField],
                             field: String,
                             value: NSDbType): Either[String, Query] = {
    schema.get(field) match {
      case Some(SchemaField(_, _, _: VARCHAR)) =>
        Right(new WildcardQuery(new Term(field, value.rawValue.toString.replaceAll("\\$", "*"))))
      case Some(_) =>
        Left(StatementParserErrors.nonCompatibleOperator("Like", "VARCHAR"))
      case None => Left(StatementParserErrors.notExistingField(field))
    }
  }

  private def comparisonExpression[T](schema: Map[String, SchemaField],
                                      field: String,
                                      operator: ComparisonOperator,
                                      value: NSDbType): Either[String, Query] = {
    def buildRangeQuery[T](fieldTypeRangeQuery: (String, T, T) => Query,
                           greaterF: T,
                           lessThan: T,
                           min: T,
                           max: T,
                           v: T): Query =
      operator match {
        case GreaterThanOperator      => fieldTypeRangeQuery(field, greaterF, max)
        case GreaterOrEqualToOperator => fieldTypeRangeQuery(field, v, max)
        case LessThanOperator         => fieldTypeRangeQuery(field, min, lessThan)
        case LessOrEqualToOperator    => fieldTypeRangeQuery(field, min, v)
      }

    (schema.get(field), value.rawValue) match {
      case (Some(SchemaField(_, _, t: INT)), v) =>
        Try(t.cast(v)).map(v =>
          buildRangeQuery[Int](IntPoint.newRangeQuery, v + 1, v - 1, Int.MinValue, Int.MaxValue, v)) match {
          case Success(q) => Right(q)
          case _          => Left(StatementParserErrors.nonCompatibleOperator("range", "INT"))
        }
      case (Some(SchemaField(_, _, t: BIGINT)), v) =>
        Try(t.cast(v)).map(v =>
          buildRangeQuery[Long](LongPoint.newRangeQuery, v + 1, v - 1, Long.MinValue, Long.MaxValue, v)) match {
          case Success(q) => Right(q)
          case _ =>
            Left(StatementParserErrors.nonCompatibleOperator("range", "BIGINT"))
        }
      case (Some(SchemaField(_, _, t: DECIMAL)), v) =>
        Try(t.cast(v)).map(
          v =>
            buildRangeQuery[Double](DoublePoint.newRangeQuery,
                                    Math.nextAfter(v, Double.MaxValue),
                                    Math.nextAfter(v, Double.MinValue),
                                    Double.MinValue,
                                    Double.MaxValue,
                                    v)) match {
          case Success(q) => Right(q)
          case _          => Left(StatementParserErrors.nonCompatibleOperator("range", "DECIMAL"))
        }
      case (Some(_), _) =>
        Left(StatementParserErrors.nonCompatibleOperator("comparison", "numerical"))
      case (None, _) => Left(StatementParserErrors.notExistingField(field))
    }
  }

  private def rangeExpression(schema: Map[String, SchemaField],
                              field: String,
                              p1: NSDbType,
                              p2: NSDbType): Either[String, Query] = {
    (schema.get(field), p1.rawValue, p2.rawValue) match {
      case (Some(SchemaField(_, _, t: BIGINT)), v1, v2) =>
        Try((t.cast(v1), t.cast(v2))).map {
          case (l, h) => LongPoint.newRangeQuery(field, l, h)
        } match {
          case Success(q) => Right(q)
          case _ =>
            Left(StatementParserErrors.nonCompatibleOperator("range", "BIGINT"))
        }
      case (Some(SchemaField(_, _, t: INT)), v1, v2) =>
        Try((t.cast(v1), t.cast(v2))).map {
          case (l, h) => IntPoint.newRangeQuery(field, l, h)
        } match {
          case Success(q) => Right(q)
          case _          => Left(StatementParserErrors.nonCompatibleOperator("range", "INT"))
        }
      case (Some(SchemaField(_, _, t: DECIMAL)), v1, v2) =>
        Try((t.cast(v1), t.cast(v2))).map {
          case (l, h) => DoublePoint.newRangeQuery(field, l, h)
        } match {
          case Success(q) => Right(q)
          case _ =>
            Left(StatementParserErrors.nonCompatibleOperator("range", "DECIMAL"))
        }
      case (Some(SchemaField(_, _, _: VARCHAR)), _, _) =>
        Left(StatementParserErrors.nonCompatibleOperator("range", "numerical"))
      case (None, _, _) => Left(StatementParserErrors.notExistingField(field))
    }
  }

  private def unaryLogicalExpression(schema: Map[String, SchemaField], expression: Expression)(
      implicit timeContext: TimeContext): Either[String, BooleanQuery] = {
    parseExpression(Some(expression), schema).map { e =>
      val builder = new BooleanQuery.Builder()
      builder.add(new MatchAllDocsQuery(), BooleanClause.Occur.MUST)
      builder.add(e.q, BooleanClause.Occur.MUST_NOT).build()
    }
  }

  private def tupledLogicalExpression(
      schema: Map[String, SchemaField],
      expression1: Expression,
      operator: TupledLogicalOperator,
      expression2: Expression)(implicit timeContext: TimeContext): Either[String, BooleanQuery] = {
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
  }
}
