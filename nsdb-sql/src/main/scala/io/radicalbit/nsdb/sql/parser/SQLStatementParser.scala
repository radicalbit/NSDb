package io.radicalbit.nsdb.sql.parser

import io.radicalbit.nsdb.common.JSerializable
import io.radicalbit.nsdb.common.statement._

import scala.util.parsing.combinator.{PackratParsers, RegexParsers}
import scala.util.{Try, Failure => ScalaFailure, Success => ScalaSuccess}

final class SQLStatementParser extends RegexParsers with PackratParsers {

  implicit class InsensitiveString(str: String) {
    def ignoreCase: Parser[String] = ("""(?i)\Q""" + str + """\E""").r ^^ { _.toString.toUpperCase }
  }

  private val Insert           = "INSERT INTO" ignoreCase
  private val Dim              = "DIM" ignoreCase
  private val Ts               = "TS" ignoreCase
  private val Val              = "VAL" ignoreCase
  private val Select           = "SELECT" ignoreCase
  private val Delete           = "DELETE" ignoreCase
  private val Drop             = "Drop" ignoreCase
  private val All              = "*"
  private val From             = "FROM" ignoreCase
  private val Where            = "WHERE" ignoreCase
  private val Comma            = ","
  private val In               = "IN" ignoreCase
  private val Order            = "ORDER BY" ignoreCase
  private val Asc              = "ASC" ignoreCase
  private val Desc             = "DESC" ignoreCase
  private val DescLiteral      = "DESC"
  private val Limit            = "LIMIT" ignoreCase
  private val GreaterThan      = ">"
  private val GreaterOrEqualTo = ">="
  private val LessThan         = "<"
  private val LessOrEqualTo    = "<="
  private val Equal            = "="
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

  private val digits     = """(^(?!now)[a-zA-Z_][a-zA-Z0-9_]*)""".r
  private val numbers    = """([0-9]+)""".r
  private val intValue   = numbers ^^ { _.toInt }
  private val longValue  = numbers ^^ { _.toLong }
  private val floatValue = """([0-9]+)\.([0-9]+)""".r ^^ { _.toFloat }

  private val field = digits ^^ { e =>
    Field(e, None)
  }
  private val aggField = ((sum | min | max | count) <~ OpenRoundBracket) ~ digits <~ CloseRoundBracket ^^ { e =>
    Field(e._2, Some(e._1))
  }
  private val metric      = """(^[a-zA-Z][a-zA-Z0-9_]*)""".r
  private val dimension   = digits
  private val stringValue = digits

  private val timeMeasure = ("h".ignoreCase | "m".ignoreCase | "s".ignoreCase).map(_.toUpperCase()) ^^ {
    case "H" => 3600 * 1000
    case "M" => 60 * 1000
    case "S" => 1000
  }

  private val delta = now ~> ("+" | "-") ~ longValue ~ timeMeasure ^^ {
    case "+" ~ v ~ measure => System.currentTimeMillis() + v * measure
    case "-" ~ v ~ measure => System.currentTimeMillis() - v * measure
  }

  private val timestamp = delta | longValue

  private val selectFields = (All | aggField | field) ~ rep(Comma ~> field) ^^ {
    case f ~ fs =>
      f match {
        case All      => AllFields
        case f: Field => ListFields(f +: fs)
      }
  }

  private val timestampAssignment = (Ts ~ Equal) ~> timestamp

  private val valueAssignment = (Val ~ Equal) ~> (floatValue | intValue)

  private val assignment = (dimension <~ Equal) ~ (stringValue | floatValue | intValue) ^^ {
    case k ~ v => k -> v.asInstanceOf[JSerializable]
  }

  private val assignments = OpenRoundBracket ~> assignment ~ rep(Comma ~> assignment) <~ CloseRoundBracket ^^ {
    case a ~ as => (a +: as).toMap
  }

  // Please don't change the order of the expressions, can cause infinite recursions
  private def expression: Parser[Expression] =
    rangeExpression | unaryLogicalExpression | tupledLogicalExpression | comparisonExpression | equalityExpression

  private def termExpression: Parser[Expression] = comparisonExpression | rangeExpression | equalityExpression

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

  private def equalityExpression = (dimension <~ Equal) ~ (stringValue | floatValue | timestamp) ^^ {
    case dim ~ v => EqualityExpression(dim, v)
  }

  private def comparisonExpression: Parser[ComparisonExpression[_]] =
    comparisonExpressionGT | comparisonExpressionGTE | comparisonExpressionLT | comparisonExpressionLTE

  private def comparisonExpression(operator: String,
                                   comparisonOperator: ComparisonOperator): Parser[ComparisonExpression[_]] =
    (dimension <~ operator) ~ timestamp ^^ {
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

  private def select = Select ~> selectFields

  private def from = From ~> metric

  private def where = Where ~> expression

  private def groupBy = (group ~> dimension) ?

  private def order = (((Order ~> dimension) ?) ~ ((Asc | Desc) ?)) ^^ {
    case dim ~(Some(ord)) if ord.equalsIgnoreCase(DescLiteral) => dim.map(DescOrderOperator)
    case dim ~ _                                               => dim.map(AscOrderOperator)
  }

  private def limit = ((Limit ~> intValue) ?) ^^ (value => value.map(x => LimitOperator(x)))

  private def selectQuery(namespace: String) = select ~ from ~ (where ?) ~ groupBy ~ order ~ limit <~ ";" ^^ {
    case fs ~ met ~ cond ~ group ~ ord ~ lim =>
      SelectSQLStatement(namespace = namespace,
                         metric = met,
                         fields = fs,
                         condition = cond.map(Condition),
                         groupBy = group,
                         order = ord,
                         limit = lim)
  }

  private def deleteQuery(namespace: String) = Delete ~> from ~ where ^^ {
    case met ~ cond => DeleteSQLStatement(namespace = namespace, metric = met, condition = Condition(cond))
  }

  private def dropStatement(namespace: String) = Drop ~> metric ^^ {
    case met => DropSQLStatement(namespace = namespace, metric = met)
  }

  private def insertQuery(namespace: String) =
    (Insert ~> metric) ~
      (timestampAssignment ?) ~
      (Dim ~> assignments) ~ valueAssignment ^^ {
      case met ~ ts ~ dimensions ~ value =>
        InsertSQLStatement(namespace = namespace,
                           metric = met,
                           timestamp = ts,
                           ListAssignment(dimensions),
                           value.asInstanceOf[JSerializable])
    }

  private def query(namespace: String): Parser[SQLStatement] =
    selectQuery(namespace) | insertQuery(namespace) | deleteQuery(namespace) | dropStatement(namespace)

  def parse(namespace: String, input: String): Try[SQLStatement] =
    Try(parse(query(namespace), s"$input;")) flatMap {
      case Success(res, _) => ScalaSuccess(res)
      case Error(msg, _)   => ScalaFailure(new RuntimeException(msg))
      case Failure(msg, _) => ScalaFailure(new RuntimeException(msg))
    }
}
