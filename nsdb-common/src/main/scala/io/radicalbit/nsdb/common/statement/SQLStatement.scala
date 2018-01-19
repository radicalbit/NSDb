package io.radicalbit.nsdb.common.statement

import io.radicalbit.nsdb.common.JSerializable

import scala.util.Try

case class Field(name: String, aggregation: Option[Aggregation])

sealed trait SelectedFields
case object AllFields                      extends SelectedFields
case class ListFields(fields: List[Field]) extends SelectedFields

case class ListAssignment(fields: Map[String, JSerializable])
case class Condition(expression: Expression)

sealed trait Expression
case class UnaryLogicalExpression(expression: Expression, operator: SingleLogicalOperator) extends Expression
case class TupledLogicalExpression(expression1: Expression, operator: TupledLogicalOperator, expression2: Expression)
    extends Expression
case class ComparisonExpression[T](dimension: String, comparison: ComparisonOperator, value: T) extends Expression
case class RangeExpression[T](dimension: String, value1: T, value2: T)                          extends Expression
case class EqualityExpression[T](dimension: String, value: T)                                   extends Expression
case class LikeExpression(dimension: String, value: String)                                     extends Expression

sealed trait LogicalOperator
sealed trait SingleLogicalOperator extends LogicalOperator
case object NotOperator            extends SingleLogicalOperator
sealed trait TupledLogicalOperator extends LogicalOperator
case object AndOperator            extends TupledLogicalOperator
case object OrOperator             extends TupledLogicalOperator

sealed trait ComparisonOperator
case object GreaterThanOperator      extends ComparisonOperator
case object GreaterOrEqualToOperator extends ComparisonOperator
case object LessThanOperator         extends ComparisonOperator
case object LessOrEqualToOperator    extends ComparisonOperator

sealed trait Aggregation
case object CountAggregation extends Aggregation
case object MaxAggregation   extends Aggregation
case object MinAggregation   extends Aggregation
case object SumAggregation   extends Aggregation

sealed trait OrderOperator {
  def dimension: String
}
case class AscOrderOperator(override val dimension: String)  extends OrderOperator
case class DescOrderOperator(override val dimension: String) extends OrderOperator

case class LimitOperator(value: Int)

sealed trait SQLStatement extends NSDBStatement {
  def db: String
  def namespace: String
  def metric: String
}

case class SelectSQLStatement(override val db: String,
                              override val namespace: String,
                              override val metric: String,
                              distinct: Boolean,
                              fields: SelectedFields,
                              condition: Option[Condition] = None,
                              groupBy: Option[String] = None,
                              order: Option[OrderOperator] = None,
                              limit: Option[LimitOperator] = None)
    extends SQLStatement {

  def enrichWithTimeRange(dimension: String, from: Long, to: Long): SelectSQLStatement = {
    val tsRangeExpression = RangeExpression(dimension, from, to)
    val newCondition = this.condition match {
      case Some(cond) => Condition(TupledLogicalExpression(tsRangeExpression, AndOperator, cond.expression))
      case None       => Condition(tsRangeExpression)
    }
    this.copy(condition = Some(newCondition))
  }

  private def filterToExpression(dimension: String, value: JSerializable, operator: String): Expression = {
    operator.toUpperCase match {
      case ">"    => ComparisonExpression(dimension, GreaterThanOperator, value)
      case ">="   => ComparisonExpression(dimension, GreaterOrEqualToOperator, value)
      case "="    => EqualityExpression(dimension, value)
      case "<="   => ComparisonExpression(dimension, LessOrEqualToOperator, value)
      case "<"    => ComparisonExpression(dimension, LessThanOperator, value)
      case "LIKE" => LikeExpression(dimension, value.asInstanceOf[String])
    }
  }

  def addConditions(filters: Seq[(String, JSerializable, String)]): SelectSQLStatement = {
    val expressions: Seq[Expression] = filters.map(f => filterToExpression(f._1, f._2, f._3))
    val filtersExpression =
      expressions.reduce((prevExpr, expr) => TupledLogicalExpression(prevExpr, AndOperator, expr))
    val newCondition: Condition = this.condition match {
      case Some(cond) => Condition(TupledLogicalExpression(cond.expression, AndOperator, filtersExpression))
      case None       => Condition(filtersExpression)
    }
    this.copy(condition = Some(newCondition))
  }

  def getTimeOrdering: Option[Ordering[Long]] =
    this.order.collect {
      case o: AscOrderOperator if o.dimension == "timestamp"  => implicitly[Ordering[Long]]
      case o: DescOrderOperator if o.dimension == "timestamp" => implicitly[Ordering[Long]].reverse
    }

}

case class InsertSQLStatement(override val db: String,
                              override val namespace: String,
                              override val metric: String,
                              timestamp: Option[Long],
                              dimensions: Option[ListAssignment],
                              value: JSerializable)
    extends SQLStatement

case class DeleteSQLStatement(override val db: String,
                              override val namespace: String,
                              override val metric: String,
                              condition: Condition)
    extends SQLStatement

case class DropSQLStatement(override val db: String, override val namespace: String, override val metric: String)
    extends SQLStatement
