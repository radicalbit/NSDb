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
import io.radicalbit.nsdb.index.{BIGINT, DECIMAL, INT, VARCHAR}
import io.radicalbit.nsdb.model.SchemaField
import org.apache.lucene.document.{DoublePoint, IntPoint, LongPoint}
import org.apache.lucene.index.Term
import org.apache.lucene.search._

import scala.util.{Failure, Success, Try}

object ExpressionParser {

  protected case class ParsedExpression(q: Query)

  /**
    * Parses an optional [[Expression]] into a [[ParsedExpression]].
    * @param exp the expression to be parsed.
    * @param schema metric'groupFieldType schema fields map.
    * @return a Try of [[ParsedExpression]] to potential errors.
    */
  def parseExpression(exp: Option[Expression], schema: Map[String, SchemaField]): Try[ParsedExpression] = {
    val q = exp match {
      case Some(NullableExpression(dimension)) => nullableExpression(schema, dimension)

      case Some(EqualityExpression(dimension, ComparisonValue(value))) => equalityExpression(schema, dimension, value)
      case Some(LikeExpression(dimension, value))                      => likeExpression(schema, dimension, value)
      case Some(ComparisonExpression(dimension, operator: ComparisonOperator, ComparisonValue(value))) =>
        comparisonExpression(schema, dimension, operator, value)
      case Some(RangeExpression(dimension, ComparisonValue(value1), ComparisonValue(value2))) =>
        rangeExpression(schema, dimension, value1, value2)

      case Some(UnaryLogicalExpression(expression, _)) => unaryLogicalExpression(schema, expression)

      case Some(TupledLogicalExpression(expression1, operator: TupledLogicalOperator, expression2: Expression)) =>
        tupledLogicalExpression(schema, expression1, operator, expression2)

      case None => Success(new MatchAllDocsQuery())
    }

    q.map(ParsedExpression)
  }

  private def nullableExpression(schema: Map[String, SchemaField], field: String) = {
    val query = schema.get(field) match {
      case Some(SchemaField(_, _, _: INT)) =>
        Try(IntPoint.newRangeQuery(field, Int.MinValue, Int.MaxValue))
      case Some(SchemaField(_, _, _: BIGINT)) =>
        Try(LongPoint.newRangeQuery(field, Long.MinValue, Long.MaxValue))
      case Some(SchemaField(_, _, _: DECIMAL)) =>
        Try(DoublePoint.newRangeQuery(field, Double.MinValue, Double.MaxValue))
      case Some(SchemaField(_, _, _: VARCHAR)) => Try(new WildcardQuery(new Term(field, "*")))
      case None                                => Failure(new InvalidStatementException(StatementParserErrors.notExistingDimension(field)))
    }
    // Used to apply negation due to the fact Lucene does not support nullable fields, query the values' range and apply negation
    query.map { qq =>
      val builder = new BooleanQuery.Builder()
      builder.add(new MatchAllDocsQuery(), BooleanClause.Occur.MUST)
      builder.add(qq, BooleanClause.Occur.MUST_NOT).build()
    }
  }

  private def equalityExpression(schema: Map[String, SchemaField], field: String, value: Any): Try[Query] = {
    schema.get(field) match {
      case Some(SchemaField(_, _, t: INT)) =>
        Try(IntPoint.newExactQuery(field, t.cast(value))) recoverWith {
          case _ =>
            Failure(new InvalidStatementException(StatementParserErrors.uncompatibleOperator("equality", "INT")))
        }
      case Some(SchemaField(_, _, t: BIGINT)) =>
        Try(LongPoint.newExactQuery(field, t.cast(value))) recoverWith {
          case _ =>
            Failure(new InvalidStatementException(StatementParserErrors.uncompatibleOperator("equality", "BIGINT")))
        }
      case Some(SchemaField(_, _, t: DECIMAL)) =>
        Try(DoublePoint.newExactQuery(field, t.cast(value))) recoverWith {
          case _ =>
            Failure(new InvalidStatementException(StatementParserErrors.uncompatibleOperator("equality", "DECIMAL")))
        }
      case Some(SchemaField(_, _, _: VARCHAR)) => Try(new TermQuery(new Term(field, value.toString)))
      case None                                => Failure(new InvalidStatementException(StatementParserErrors.notExistingDimension(field)))
    }
  }

  private def likeExpression(schema: Map[String, SchemaField], field: String, value: String): Try[WildcardQuery] = {
    schema.get(field) match {
      case Some(SchemaField(_, _, _: VARCHAR)) =>
        Success(new WildcardQuery(new Term(field, value.replaceAll("\\$", "*"))))
      case Some(_) =>
        Failure(new InvalidStatementException(StatementParserErrors.uncompatibleOperator("Like", "VARCHAR")))
      case None => Failure(new InvalidStatementException(StatementParserErrors.notExistingDimension(field)))
    }
  }

  private def comparisonExpression[T](schema: Map[String, SchemaField],
                                      field: String,
                                      operator: ComparisonOperator,
                                      value: Any): Try[Query] = {
    def buildRangeQuery[T](fieldTypeRangeQuery: (String, T, T) => Query,
                           greaterF: T,
                           lessThan: T,
                           min: T,
                           max: T,
                           v: T): Try[Query] =
      Success(operator match {
        case GreaterThanOperator      => fieldTypeRangeQuery(field, greaterF, max)
        case GreaterOrEqualToOperator => fieldTypeRangeQuery(field, v, max)
        case LessThanOperator         => fieldTypeRangeQuery(field, min, lessThan)
        case LessOrEqualToOperator    => fieldTypeRangeQuery(field, min, v)
      })

    (schema.get(field), value) match {
      case (Some(SchemaField(_, _, t: INT)), v) =>
        Try(t.cast(v)).flatMap(v =>
          buildRangeQuery[Int](IntPoint.newRangeQuery, v + 1, v - 1, Int.MinValue, Int.MaxValue, v)) recoverWith {
          case _ => Failure(new InvalidStatementException(StatementParserErrors.uncompatibleOperator("range", "INT")))
        }
      case (Some(SchemaField(_, _, t: BIGINT)), v) =>
        Try(t.cast(v)).flatMap(v =>
          buildRangeQuery[Long](LongPoint.newRangeQuery, v + 1, v - 1, Long.MinValue, Long.MaxValue, v)) recoverWith {
          case _ =>
            Failure(new InvalidStatementException(StatementParserErrors.uncompatibleOperator("range", "BIGINT")))
        }
      case (Some(SchemaField(_, _, t: DECIMAL)), v) =>
        Try(t.cast(v)).flatMap(
          v =>
            buildRangeQuery[Double](DoublePoint.newRangeQuery,
                                    Math.nextAfter(v, Double.MaxValue),
                                    Math.nextAfter(v, Double.MinValue),
                                    Double.MinValue,
                                    Double.MaxValue,
                                    v)) recoverWith {
          case _ =>
            Failure(new InvalidStatementException(StatementParserErrors.uncompatibleOperator("range", "DECIMAL")))
        }
      case (Some(_), _) =>
        Failure(new InvalidStatementException(StatementParserErrors.uncompatibleOperator("comparison", "numerical")))
      case (None, _) => Failure(new InvalidStatementException(StatementParserErrors.notExistingDimension(field)))
    }
  }

  private def rangeExpression(schema: Map[String, SchemaField], field: String, p1: Any, p2: Any): Try[Query] = {
    (schema.get(field), p1, p2) match {
      case (Some(SchemaField(_, _, t: BIGINT)), v1, v2) =>
        Try((t.cast(v1), t.cast(v2))).map {
          case (l, h) => LongPoint.newRangeQuery(field, l, h)
        } recoverWith {
          case _ =>
            Failure(new InvalidStatementException(StatementParserErrors.uncompatibleOperator("range", "BIGINT")))
        }
      case (Some(SchemaField(_, _, t: INT)), v1, v2) =>
        Try((t.cast(v1), t.cast(v2))).map {
          case (l, h) => IntPoint.newRangeQuery(field, l, h)
        } recoverWith {
          case _ => Failure(new InvalidStatementException(StatementParserErrors.uncompatibleOperator("range", "INT")))
        }
      case (Some(SchemaField(_, _, t: DECIMAL)), v1, v2) =>
        Try((t.cast(v1), t.cast(v2))).map {
          case (l, h) => DoublePoint.newRangeQuery(field, l, h)
        } recoverWith {
          case _ =>
            Failure(new InvalidStatementException(StatementParserErrors.uncompatibleOperator("range", "DECIMAL")))
        }
      case (Some(SchemaField(_, _, _: VARCHAR)), _, _) =>
        Failure(new InvalidStatementException(StatementParserErrors.uncompatibleOperator("range", "numerical")))
      case (None, _, _) => Failure(new InvalidStatementException(StatementParserErrors.notExistingDimension(field)))
    }
  }

  private def unaryLogicalExpression(schema: Map[String, SchemaField], expression: Expression): Try[BooleanQuery] = {
    parseExpression(Some(expression), schema).map { e =>
      val builder = new BooleanQuery.Builder()
      builder.add(new MatchAllDocsQuery(), BooleanClause.Occur.MUST)
      builder.add(e.q, BooleanClause.Occur.MUST_NOT).build()
    }
  }

  private def tupledLogicalExpression(schema: Map[String, SchemaField],
                                      expression1: Expression,
                                      operator: TupledLogicalOperator,
                                      expression2: Expression): Try[BooleanQuery] = {
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
