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

package io.radicalbit.nsdb.sql.parser

import io.radicalbit.nsdb.common.statement._
import io.radicalbit.nsdb.common.{NSDbNumericType, NSDbType}
import io.radicalbit.nsdb.sql.parser.StatementParserResult._

import scala.language.postfixOps
import scala.util.parsing.combinator.{PackratParsers, RegexParsers}
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
final class SQLStatementParser extends RegexParsers with PackratParsers with RegexNSDb {

  implicit class InsensitiveString(str: String) {
    def ignoreCase: PackratParser[String] = ("""(?i)\Q""" + str + """\E""").r ^^ { _.toUpperCase }
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
  private val first = "FIRST".ignoreCase ^^ { _ =>
    FirstAggregation
  }
  private val last = "LAST".ignoreCase ^^ { _ =>
    LastAggregation
  }
  private val avg = "AVG".ignoreCase ^^ { _ =>
    AvgAggregation
  }
  private val group             = "GROUP BY" ignoreCase
  private val OpenRoundBracket  = "("
  private val CloseRoundBracket = ")"
  private val temporalInterval  = "INTERVAL" ignoreCase

  private val letter          = """[a-zA-Z_]"""
  private val digit           = """[0-9]"""
  private val letterOrDigit   = """[a-zA-Z0-9_]"""
  private val specialChar     = """[a-zA-Z0-9_\-\.:~]"""
  private val specialWildCard = """[a-zA-Z0-9_\-$\.:~]"""

  private val standardString = s"""($letter$letterOrDigit*)""".r
  private val specialString  = s"""($letter$specialChar*$letterOrDigit*)""".r
  private val wildCardString = s"""($specialWildCard+)""".r
  private val digits         = s"""($digit+)""".r
  private val intValue       = digits ^^ { _.toInt }
  private val longValue      = digits ^^ { _.toLong }
  private val doubleValue    = s"""($digit+)[.]($digit+)""".r ^^ { _.toDouble }

  private val field = standardString ^^ { e =>
    Field(e, None)
  }
  private val aggField = ((sum | min | max | count | first | last | avg) <~ OpenRoundBracket) ~ (standardString | All) <~ CloseRoundBracket ^^ {
    case aggregation ~ name => Field(name, Some(aggregation))
  }
  private val dimension = standardString
  private val stringValue = (specialString | (("'" ?) ~> (specialString +) <~ ("'" ?))) ^^ {
    case string: String        => string
    case strings: List[String] => strings.mkString(" ")
  }

  private val stringValueWithWildcards = (wildCardString | (("'" ?) ~> (wildCardString +) <~ ("'" ?))) ^^ {
    case string: String        => string
    case strings: List[String] => strings.mkString(" ")
  }

  private val timeMeasure = ("d".ignoreCase | "h".ignoreCase | "m".ignoreCase | "s".ignoreCase)
    .map(_.toUpperCase()) ^^ {
    case "D" => ("d", 24 * 3600 * 1000)
    case "H" => ("h", 3600 * 1000)
    case "M" => ("m", 60 * 1000)
    case "S" => ("s", 1000)
  }

  // def instead val because timestamp should be calculated everytime but there is a problem with
  // more than one "now - something" in the same query because "now" will be different for each condition
  private def delta: PackratParser[ComparisonValue] = now ~> (("+" | "-") ~ longValue ~ timeMeasure).? ^^ {
    case Some("+" ~ v ~ ((unitMeasure, timeInterval))) =>
      RelativeComparisonValue(System.currentTimeMillis() + v * timeInterval, "+", v, unitMeasure)
    case Some("-" ~ v ~ ((unitMeasure, timeInterval))) =>
      RelativeComparisonValue(System.currentTimeMillis() - v * timeInterval, "-", v, unitMeasure)
    case None =>
      AbsoluteComparisonValue(System.currentTimeMillis())
  }

  private val comparisonTerm = delta | doubleValue.map(AbsoluteComparisonValue(_)) | longValue.map(
    AbsoluteComparisonValue(_))

  private val selectFields = (Distinct ?) ~ (All | aggField | field) ~ rep(Comma ~> (aggField | field)) ^^ {
    case _ ~ f ~ fs =>
      f match {
        case All      => AllFields()
        case f: Field => ListFields(f +: fs)
      }
  }

  private val timestampAssignment = (Ts ~ Equal) ~> longValue

  private val valueAssignment = (Val ~ Equal) ~> (doubleValue | longValue)

  private val assignment = (dimension <~ Equal) ~ (stringValue | doubleValue | intValue) ^^ {
    case k ~ v => k -> NSDbType(v)
  }

  private val assignments = OpenRoundBracket ~> assignment ~ rep(Comma ~> assignment) <~ CloseRoundBracket ^^ {
    case a ~ as => (a +: as).toMap
  }

  // Please don't change the order of the expressions, can cause infinite recursions
  private lazy val expression: PackratParser[Expression] =
    unaryLogicalExpression | tupledLogicalExpression | nullableExpression | rangeExpression | comparisonExpressionRule | equalityExpression | likeExpression | bracketedExpression

  private lazy val bracketedExpression: PackratParser[Expression] = OpenRoundBracket ~> expression <~ CloseRoundBracket

  private lazy val unaryLogicalExpression = notUnaryLogicalExpression

  private lazy val notUnaryLogicalExpression: PackratParser[NotExpression] =
    (Not ~> expression) ^^ (expression => NotExpression(expression))

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

  lazy val equalityExpression: PackratParser[EqualityExpression] = (dimension <~ Equal) ~ (comparisonTerm | stringValue
    .map(n => AbsoluteComparisonValue(n))) ^^ {
    case dim ~ v => EqualityExpression(dim, v)
  }

  lazy val likeExpression: PackratParser[LikeExpression] =
    (dimension <~ Like) ~ stringValueWithWildcards ^^ {
      case dim ~ v => LikeExpression(dim, v)
    }

  lazy val nullableExpression: PackratParser[Expression] =
    (dimension <~ Is) ~ (Not ?) ~ Null ^^ {
      case dim ~ Some(_) ~ _ => NotExpression(NullableExpression(dim))
      case dim ~ None ~ _    => NullableExpression(dim)
    }

  private lazy val comparisonExpressionRule = comparisonExpressionGT | comparisonExpressionGTE | comparisonExpressionLT | comparisonExpressionLTE

  private def comparisonExpression(operator: String,
                                   comparisonOperator: ComparisonOperator): Parser[ComparisonExpression] =
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

  lazy val select: PackratParser[~[Option[String], SelectedFields]] = Select ~> Distinct.? ~ selectFields

  lazy val from: PackratParser[String] = From ~> metric

  lazy val where: PackratParser[Expression] = Where ~> expression

  lazy val simpleGroupBy: PackratParser[GroupByAggregation] = (group ~> dimension) ^^ { dim =>
    SimpleGroupByAggregation(dim)
  }

  lazy val temporalGroupBy
    : PackratParser[GroupByAggregation] = (group ~> temporalInterval ~> (intValue ?) ~ timeMeasure) ^^ {
    case unit ~ ((timeMeasure, quantity)) =>
      TemporalGroupByAggregation(unit.getOrElse(1) * quantity, unit.getOrElse(1).toLong, timeMeasure)
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
          value = NSDbNumericType(value)
        )
    }

  private def query(db: String, namespace: String): PackratParser[SQLStatement] =
    selectQuery(db, namespace) | insertQuery(db, namespace) | deleteQuery(db, namespace) | dropStatement(db, namespace)

  def parse(db: String, namespace: String, input: String): SqlStatementParserResult = {
    parse(query(db, namespace), new PackratReader[Char](new CharSequenceReader(s"$input;"))) match {
      case Success(statement, _) => SqlStatementParserSuccess(input, statement)
      case Error(msg, next) =>
        SqlStatementParserFailure(input, s"$msg \n ${next.source.toString.takeRight(next.offset)}")
      case Failure(msg, next) =>
        SqlStatementParserFailure(input, s"$msg \n ${next.source.toString.takeRight(next.offset)}")
    }
  }

}
