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

package io.radicalbit.nsdb.sql.parser

import io.radicalbit.nsdb.common.JSerializable
import io.radicalbit.nsdb.common.exception.InvalidStatementException
import io.radicalbit.nsdb.common.statement._

import scala.util.parsing.combinator.{PackratParsers, RegexParsers}
import scala.util.{Try, Failure => ScalaFailure, Success => ScalaSuccess}
import scala.language.postfixOps
import scala.util.parsing.input.CharSequenceReader

/**
  * Parser combinator for sql statements.
  * It successfully validates and parses a subset of the Ansi Sql grammar.
  * It mixes in [[PackratParsers]] which turns the parser into a look ahead parser which can handle left recursive grammars
  * Here is the (simplified) EBNF grammar supported by Nsdb Parser.
  * {{{
  *   S := InsertStatement | SelectStatement | DeleteStatement | DropStatement
  *   InsertStatement := "insert into" literal ("ts =" digit)? "dim" "(" (literal = literal)+ ")" "tags" "(" (literal = literal)+ ")" "VAL" = digit
  *   DropStatement := "drop metric" literal
  *   DeleteStatement := "delete" "from" literal ("where" expression)?
  *   SelectStatement := "select" "distinct"? selectFields "from" literal ("where" expression)? ("group by" (literal |  digit? timeMeasure))? ("order by" literal ("desc")?)? (limit digit)?
  *   selectFields := "*" | aggregation(literal | "*") | (literal | "*")+
  *   expression := expression "and" expression | expression "or" expression | "not" expression | "(" expression ")"
  *                 literal "=" literal | literal comparison literal | literal "like" literal |
  *                 literal "in" "(" digit "," digit ")" | literal "is" "not"? "null"
  *   comparison := "=" | ">" | "<" | ">=" | "<="
  *   timeMeasure := "D" | "H" | "M" | "S"
  * }}}
  */
final class SQLStatementParser extends RegexParsers with PackratParsers {

  implicit class InsensitiveString(str: String) {
    def ignoreCase: PackratParser[String] = ("""(?i)\Q""" + str + """\E""").r ^^ { _.toString.toUpperCase }
  }

  private val Insert           = "INSERT INTO" ignoreCase
  private val Dim              = "DIM" ignoreCase
  private val Tags             = "TAGS" ignoreCase
  private val Ts               = "TS" ignoreCase
  private val Val              = "VAL" ignoreCase
  private val Select           = "SELECT" ignoreCase
  private val Delete           = "DELETE" ignoreCase
  private val Drop             = "DROP METRIC" ignoreCase
  private val All              = "*"
  private val From             = "FROM" ignoreCase
  private val Where            = "WHERE" ignoreCase
  private val Distinct         = "DISTINCT" ignoreCase
  private val Comma            = ","
  private val In               = "IN" ignoreCase
  private val Order            = "ORDER BY" ignoreCase
  private val Desc             = "DESC" ignoreCase
  private val Limit            = "LIMIT" ignoreCase
  private val GreaterThan      = ">"
  private val GreaterOrEqualTo = ">="
  private val LessThan         = "<"
  private val LessOrEqualTo    = "<="
  private val Equal            = "="
  private val Like             = "LIKE" ignoreCase
  private val Is               = "is" ignoreCase
  private val Null             = "null" ignoreCase
  private val Not              = "NOT" ignoreCase
  private val And              = "AND" ignoreCase
  private val Or               = "OR" ignoreCase
  private val now              = "NOW" ignoreCase
  private val sum = "SUM".ignoreCase ^^ { _ =>
    SumAggregation
  }
  private val min = "MIN".ignoreCase ^^ { _ =>
    MinAggregation
  }
  private val max = "MAX".ignoreCase ^^ { _ =>
    MaxAggregation
  }
  private val count = "COUNT".ignoreCase ^^ { _ =>
    CountAggregation
  }
  private val group             = "GROUP BY" ignoreCase
  private val OpenRoundBracket  = "("
  private val CloseRoundBracket = ")"
  private val temporalInterval  = "INTERVAL" ignoreCase

  private val digits           = """(^(?!now)[a-zA-Z_][a-zA-Z0-9_]*)""".r
  private val digitsWithDashes = """(^(?!now)[a-zA-Z_][a-zA-Z0-9_\-]*[a-zA-Z0-9]*)""".r
  private val numbers          = """([0-9]+)""".r
  private val intValue         = numbers ^^ { _.toInt }
  private val longValue        = numbers ^^ { _.toLong }
  private val floatValue       = """([0-9]+)\.([0-9]+)""".r ^^ { _.toDouble }

  private val field = digits ^^ { e =>
    Field(e, None)
  }
  private val aggField = ((sum | min | max | count) <~ OpenRoundBracket) ~ (digits | All) <~ CloseRoundBracket ^^ { e =>
    Field(e._2, Some(e._1))
  }
  private val metric    = """(^[a-zA-Z][a-zA-Z0-9_]*)""".r
  private val dimension = digits
  private val stringValue = (digitsWithDashes | (("'" ?) ~> (digitsWithDashes +) <~ ("'" ?))) ^^ {
    case string: String        => string
    case strings: List[String] => strings.mkString(" ")
  }
  private val stringValueWithWildcards = """(^[a-zA-Z_$][a-zA-Z0-9_\-$]*[a-zA-Z0-9$])""".r

  private val timeMeasure = ("d".ignoreCase | "h".ignoreCase | "m".ignoreCase | "s".ignoreCase)
    .map(_.toUpperCase()) ^^ {
    case "D" => ("D", 24 * 3600 * 1000)
    case "H" => ("H", 3600 * 1000)
    case "M" => ("M", 60 * 1000)
    case "S" => ("S", 1000)
  }

  // def instead val because timestamp should be calculated everytime but there is a problem with
  // more than one "now - something" in the same query because "now" will be different for each condition
  private def delta: Parser[RelativeTimestampValue] = now ~> ("+" | "-") ~ longValue ~ timeMeasure ^^ {
    case "+" ~ v ~ ((unitMeasure, timeInterval)) =>
      RelativeTimestampValue(System.currentTimeMillis() + v * timeInterval, "+", v, unitMeasure)
    case "-" ~ v ~ ((unitMeasure, timeInterval)) =>
      RelativeTimestampValue(System.currentTimeMillis() - v * timeInterval, "-", v, unitMeasure)
  }

  private val comparisonTerm = delta | floatValue | longValue

  private val selectFields = (Distinct ?) ~ (All | aggField | field) ~ rep(Comma ~> (aggField | field)) ^^ {
    case _ ~ f ~ fs =>
      f match {
        case All      => AllFields
        case f: Field => ListFields(f +: fs)
      }
  }

  private val timestampAssignment = (Ts ~ Equal) ~> longValue

  private val valueAssignment = (Val ~ Equal) ~> (floatValue | longValue)

  private val assignment = (dimension <~ Equal) ~ (stringValue | floatValue | intValue) ^^ {
    case k ~ v => k -> v.asInstanceOf[JSerializable]
  }

  private val assignments = OpenRoundBracket ~> assignment ~ rep(Comma ~> assignment) <~ CloseRoundBracket ^^ {
    case a ~ as => (a +: as).toMap
  }

  // Please don't change the order of the expressions, can cause infinite recursions
  private lazy val expression: PackratParser[Expression] =
    unaryLogicalExpression | tupledLogicalExpression | nullableExpression | rangeExpression | comparisonExpression | equalityExpression | likeExpression | bracketedExpression

  private lazy val bracketedExpression: PackratParser[Expression] = OpenRoundBracket ~> expression <~ CloseRoundBracket

  private lazy val unaryLogicalExpression = notUnaryLogicalExpression

  private lazy val notUnaryLogicalExpression: PackratParser[UnaryLogicalExpression] =
    (Not ~> expression) ^^ (expression => UnaryLogicalExpression(expression, NotOperator))

  private lazy val tupledLogicalExpression: PackratParser[TupledLogicalExpression] =
    andTupledLogicalExpression | orTupledLogicalExpression

  private def tupledLogicalExpression(operator: PackratParser[String],
                                      tupledOperator: TupledLogicalOperator): PackratParser[TupledLogicalExpression] =
    (expression <~ operator) ~ expression ^^ {
      case expression1 ~ expression2 =>
        TupledLogicalExpression(expression1, tupledOperator, expression2)
    }

  lazy val andTupledLogicalExpression: PackratParser[TupledLogicalExpression] =
    tupledLogicalExpression(And, AndOperator)

  lazy val orTupledLogicalExpression: PackratParser[TupledLogicalExpression] = tupledLogicalExpression(Or, OrOperator)

  lazy val equalityExpression
    : PackratParser[EqualityExpression[Any]] = (dimension <~ Equal) ~ (stringValue | comparisonTerm) ^^ {
    case dim ~ v => EqualityExpression(dim, v)
  }

  lazy val likeExpression: PackratParser[LikeExpression] =
    (dimension <~ Like) ~ stringValueWithWildcards ^^ {
      case dim ~ v => LikeExpression(dim, v)
    }

  lazy val nullableExpression: PackratParser[Expression] =
    (dimension <~ Is) ~ (Not ?) ~ Null ^^ {
      case dim ~ Some(_) ~ _ => UnaryLogicalExpression(NullableExpression(dim), NotOperator)
      case dim ~ None ~ _    => NullableExpression(dim)
    }

  lazy val comparisonExpression: PackratParser[ComparisonExpression[_]] =
    comparisonExpressionGT | comparisonExpressionGTE | comparisonExpressionLT | comparisonExpressionLTE

  private def comparisonExpression(operator: String,
                                   comparisonOperator: ComparisonOperator): PackratParser[ComparisonExpression[_]] =
    (dimension <~ operator) ~ comparisonTerm ^^ {
      case d ~ v =>
        ComparisonExpression(d, comparisonOperator, v)
    }

  private lazy val comparisonExpressionGT = comparisonExpression(GreaterThan, GreaterThanOperator)

  private lazy val comparisonExpressionGTE = comparisonExpression(GreaterOrEqualTo, GreaterOrEqualToOperator)

  private lazy val comparisonExpressionLT = comparisonExpression(LessThan, LessThanOperator)

  private lazy val comparisonExpressionLTE = comparisonExpression(LessOrEqualTo, LessOrEqualToOperator)

  private lazy val rangeExpression =
    (dimension <~ In) ~ (OpenRoundBracket ~> comparisonTerm) ~ (Comma ~> comparisonTerm <~ CloseRoundBracket) ^^ {
      case d ~ v1 ~ v2 => RangeExpression(dimension = d, value1 = v1, value2 = v2)
    }

  lazy val select
    : PackratParser[~[Option[String], SelectedFields with Product with Serializable]] = Select ~> Distinct.? ~ selectFields

  lazy val from: PackratParser[String] = From ~> metric

  lazy val where: PackratParser[Expression] = Where ~> expression

  lazy val simpleGroupBy: PackratParser[GroupByAggregation] = (group ~> dimension) ^^ { dim =>
    SimpleGroupByAggregation(dim)
  }

  lazy val temporalGroupBy
    : PackratParser[GroupByAggregation] = (group ~> temporalInterval ~> (intValue ?) ~ timeMeasure) ^^ {
    case unit ~ conversion => TemporalGroupByAggregation(unit.getOrElse(1) * conversion._2)
  }

  lazy val order: PackratParser[OrderOperator] = (Order ~> dimension ~ (Desc ?)) ^^ {
    case dim ~ Some(_) => DescOrderOperator(dim)
    case dim ~ None    => AscOrderOperator(dim)
  }

  lazy val limit: PackratParser[LimitOperator] = (Limit ~> intValue) ^^ (value => LimitOperator(value))

  private def selectQuery(db: String, namespace: String) =
    select ~ from ~ (where ?) ~ ((temporalGroupBy | simpleGroupBy) ?) ~ (order ?) ~ (limit ?) <~ ";" ^^ {
      case d ~ fs ~ met ~ cond ~ gr ~ ord ~ lim =>
        SelectSQLStatement(db = db,
                           namespace = namespace,
                           metric = met,
                           distinct = d.isDefined,
                           fields = fs,
                           condition = cond.map(Condition),
                           groupBy = gr,
                           order = ord,
                           limit = lim)
    }

  private def deleteQuery(db: String, namespace: String) = Delete ~> from ~ where ^^ {
    case met ~ cond => DeleteSQLStatement(db = db, namespace = namespace, metric = met, condition = Condition(cond))
  }

  private def dropStatement(db: String, namespace: String) = Drop ~> metric ^^ { met =>
    DropSQLStatement(db = db, namespace = namespace, metric = met)
  }

  private def insertQuery(db: String, namespace: String) =
    (Insert ~> metric) ~
      (timestampAssignment ?) ~
      (Dim ~> assignments ?) ~
      (Tags ~> assignments ?) ~ valueAssignment ^^ {
      case met ~ ts ~ dimensions ~ tags ~ value =>
        InsertSQLStatement(
          db = db,
          namespace = namespace,
          metric = met,
          timestamp = ts,
          dimensions = dimensions.map(ListAssignment),
          tags = tags.map(ListAssignment),
          value = value.asInstanceOf[JSerializable]
        )
    }

  private def query(db: String, namespace: String): PackratParser[SQLStatement] =
    selectQuery(db, namespace) | insertQuery(db, namespace) | deleteQuery(db, namespace) | dropStatement(db, namespace)

  def parse(db: String, namespace: String, input: String): Try[SQLStatement] =
    Try(parse(query(db, namespace), new PackratReader[Char](new CharSequenceReader(s"$input;")))) flatMap {
      case Success(res, _) => ScalaSuccess(res)
      case Error(msg, next) =>
        ScalaFailure(new InvalidStatementException(s"$msg \n ${next.source.toString.takeRight(next.offset)}"))
      case Failure(msg, next) =>
        ScalaFailure(new InvalidStatementException(s"$msg \n ${next.source.toString.takeRight(next.offset)}"))
    }
}
