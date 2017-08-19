package io.radicalbit.nsdb.sql.parser

import io.radicalbit.nsdb.common.JSerializable
import io.radicalbit.nsdb.common.statement._

import scala.util.parsing.combinator.{PackratParsers, RegexParsers}
import scala.util.{Try, Failure => ScalaFailure, Success => ScalaSuccess}

final class SQLStatementParser extends RegexParsers with PackratParsers {

  implicit class InsensitiveString(str: String) {
    def ignoreCase: Parser[String] = ("""(?i)\Q""" + str + """\E""").r ^^ { _.toString.toUpperCase }
  }

  private val Insert            = "INSERT INTO" ignoreCase
  private val Dim               = "DIM" ignoreCase
  private val Ts                = "TS" ignoreCase
  private val Fld               = "FLD" ignoreCase
  private val Select            = "SELECT" ignoreCase
  private val All               = "*"
  private val From              = "FROM" ignoreCase
  private val Where             = "WHERE" ignoreCase
  private val Comma             = ","
  private val In                = "IN" ignoreCase
  private val Order             = "ORDER BY" ignoreCase
  private val Asc               = "ASC" ignoreCase
  private val Desc              = "DESC" ignoreCase
  private val DescLiteral       = "DESC"
  private val Limit             = "LIMIT" ignoreCase
  private val GreaterThan       = ">"
  private val GreaterOrEqualTo  = ">="
  private val LessThan          = "<"
  private val LessOrEqualTo     = "<="
  private val Equal             = "="
  private val Not               = "NOT" ignoreCase
  private val And               = "AND" ignoreCase
  private val Or                = "OR" ignoreCase
  private val OpenRoundBracket  = "("
  private val CloseRoundBracket = ")"

  private val field       = """(^[a-zA-Z_][a-zA-Z0-9_]+)""".r
  private val metric      = """(^[a-zA-Z_][a-zA-Z0-9_]+)""".r
  private val dimension   = """(^[a-zA-Z_][a-zA-Z0-9_]+)""".r
  private val stringValue = """(^[a-zA-Z_][a-zA-Z0-9_]+)""".r
  private val intValue    = """([0-9]+)""".r ^^ { _.toInt }
  private val timestamp   = """([0-9]+)""".r ^^ { _.toLong }

  private val selectFields = (All | field) ~ rep(Comma ~> field) ^^ {
    case f ~ fs =>
      f match {
        case All => AllFields
        case _   => ListFields(f +: fs)
      }
  }

  private val timestampAssignment = (Ts ~ Equal) ~> timestamp

  private val assignment = (field <~ Equal) ~ (stringValue | intValue) ^^ {
    case k ~ v => k -> v.asInstanceOf[JSerializable]
  }

  private val assignments = OpenRoundBracket ~> assignment ~ rep(Comma ~> assignment) <~ CloseRoundBracket ^^ {
    case a ~ as => (a +: as).toMap
  }

  // Please don't change the order of the expressions, can cause infinite recursions
  private def expression: Parser[Expression] =
    rangeExpression | unaryLogicalExpression | tupledLogicalExpression | comparisonExpression

  private def termExpression: Parser[Expression] = comparisonExpression | rangeExpression

  private def unaryLogicalExpression = notUnaryLogicalExpression

  private def notUnaryLogicalExpression =
    (Not ~> expression) ^^ (expression => UnaryLogicalExpression(expression, NotOperator))

  private def tupledLogicalExpression: Parser[TupledLogicalExpression] =
    andTupledLogicalExpression | orTupledLogicalExpression

  private def tupledLogicalExpression(operator: Parser[String],
                                      tupledOperator: TupledLogicalOperator): Parser[TupledLogicalExpression] =
    ((termExpression | expression) <~ operator) ~ (termExpression | expression) ^^ {
      case expression1 ~ expression2 =>
        TupledLogicalExpression(expression1, tupledOperator, expression2)
    }

  private def andTupledLogicalExpression = tupledLogicalExpression(And, AndOperator)

  private def orTupledLogicalExpression = tupledLogicalExpression(Or, OrOperator)

  private def comparisonExpression: Parser[ComparisonExpression[_]] =
    comparisonExpressionGT | comparisonExpressionGTE | comparisonExpressionLT | comparisonExpressionLTE

  private def comparisonExpression(operator: String,
                                   comparisonOperator: ComparisonOperator): Parser[ComparisonExpression[_]] =
    (dimension <~ operator) ~ intValue ^^ {
      case d ~ v =>
        ComparisonExpression(d, comparisonOperator, v)
    }

  private def comparisonExpressionGT = comparisonExpression(GreaterThan, GreaterThanOperator)

  private def comparisonExpressionGTE = comparisonExpression(GreaterOrEqualTo, GreaterOrEqualToOperator)

  private def comparisonExpressionLT = comparisonExpression(LessThan, LessThanOperator)

  private def comparisonExpressionLTE = comparisonExpression(LessOrEqualTo, LessOrEqualToOperator)

  private def rangeExpression =
    (dimension <~ In) ~ (OpenRoundBracket ~> timestamp) ~ (Comma ~> timestamp <~ CloseRoundBracket) ^^ {
      case (d ~ v1 ~ v2) => RangeExpression(dimension = d, value1 = v1, value2 = v2)
    }

  private def conditions = expression

  private def select = Select ~> selectFields

  private def from = From ~> metric

  private def where = (Where ~> conditions) ?

  private def order = (((Order ~> dimension) ?) ~ ((Asc | Desc) ?)) ^^ {
    case dim ~(Some(ord)) if ord.equalsIgnoreCase(DescLiteral) => dim.map(DescOrderOperator)
    case dim ~ _                                               => dim.map(AscOrderOperator)
  }

  private def limit = ((Limit ~> intValue) ?) ^^ (value => value.map(x => LimitOperator(x)))

  private def selectQuery(namespace: String) = select ~ from ~ where ~ order ~ limit ^^ {
    case fs ~ met ~ cond ~ ord ~ lim =>
      SelectSQLStatement(namespace = namespace,
                         metric = met,
                         fields = fs,
                         condition = cond.map(Condition),
                         order = ord,
                         limit = lim)
  }

  private def insertQuery(namespace: String) =
    (Insert ~> metric) ~
      (timestampAssignment ?) ~
      (Dim ~> assignments) ~ (Fld ~> assignments) ^^ {
      case met ~ ts ~ dimensions ~ fields =>
        InsertSQLStatement(namespace = namespace,
                           metric = met,
                           timestamp = ts,
                           ListAssignment(dimensions),
                           ListAssignment(fields))
    }

  private def query(namespace: String): Parser[SQLStatement] = selectQuery(namespace) | insertQuery(namespace)

  def parse(namespace: String, input: String): Try[SQLStatement] =
    Try(parse(query(namespace), input)) flatMap {
      case Success(res, _) => ScalaSuccess(res)
      case Error(msg, _)   => ScalaFailure(new RuntimeException(msg))
      case Failure(msg, _) => ScalaFailure(new RuntimeException(msg))
    }
}
