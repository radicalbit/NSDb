package io.radicalbit.nsdb.common.statement

import com.typesafe.scalalogging.LazyLogging
import io.radicalbit.nsdb.common.JSerializable

/**
  * Parsed object for sql select and insert statements.
  * @param name field's name.
  * @param aggregation if there is an aggregation for the field (sum, count ecc.).
  */
case class Field(name: String, aggregation: Option[Aggregation])

/**
  * Parsed select fields objects.
  * [[AllFields]] for a `select *` statement.
  * [[ListFields]] if a list of fields are specified in the query.
  */
sealed trait SelectedFields
case object AllFields                      extends SelectedFields
case class ListFields(fields: List[Field]) extends SelectedFields

case class ListAssignment(fields: Map[String, JSerializable])
case class Condition(expression: Expression)

/**
  * Where condition expression for queries.
  * [[ComparisonExpression]] simple comparison expression.
  */
sealed trait Expression

/**
  * Simple expression having a [[SingleLogicalOperator]] applied.
  * @param expression the simple expression.
  * @param operator the operator to be applied to the expression.
  */
case class UnaryLogicalExpression(expression: Expression, operator: SingleLogicalOperator) extends Expression

/**
  * A couple of simple expressions having a [[TupledLogicalOperator]] applied.
  * @param expression1 the first expression.
  * @param operator the operator to apply.
  * @param expression2 the second expression.
  */
case class TupledLogicalExpression(expression1: Expression, operator: TupledLogicalOperator, expression2: Expression)
    extends Expression

/**
  * Simple comparison expression described by the [[ComparisonOperator]] e.g. dimension > value.
  * @param dimension  dimension name.
  * @param comparison comparison operator (e.g. >, >=, <, <=).
  * @param value the value to compare the dimension with.
  */
case class ComparisonExpression[T](dimension: String, comparison: ComparisonOperator, value: T) extends Expression

/**
  * <b>Inclusive</b> range operation between a lower and a upper boundary.
  * @param dimension dimension name.
  * @param value1 lower boundary.
  * @param value2 upper boundary.
  */
case class RangeExpression[T](dimension: String, value1: T, value2: T) extends Expression

/**
  * Simple equality expression e.g. dimension = value.
  * @param dimension dimension name.
  * @param value value to check the equality with.
  */
case class EqualityExpression[T](dimension: String, value: T) extends Expression

/**
  * Simple like expression for varchar dimensions e.g. dimension like value.
  * @param dimension dimension name.
  * @param value string value with wildcards.
  */
case class LikeExpression(dimension: String, value: String) extends Expression

/**
  * Simple nullable expression e.g. dimension is null.
  * @param dimension dimension name.
  */
case class NullableExpression(dimension: String) extends Expression

/**
  * Logical operators that can be applied to 1 or 2 expressions.
  */
sealed trait LogicalOperator

/**
  * Logical operator that can be applied only to 1 expression e.g. [[NotOperator]].
  */
sealed trait SingleLogicalOperator extends LogicalOperator
case object NotOperator            extends SingleLogicalOperator

/**
  * Logical operators that can be applied only to 2 expressions e.g. [[AndOperator]] and [[OrOperator]].
  */
sealed trait TupledLogicalOperator extends LogicalOperator
case object AndOperator            extends TupledLogicalOperator
case object OrOperator             extends TupledLogicalOperator

/**
  * Comparison operators to be used in [[ComparisonExpression]].
  */
sealed trait ComparisonOperator
case object GreaterThanOperator      extends ComparisonOperator
case object GreaterOrEqualToOperator extends ComparisonOperator
case object LessThanOperator         extends ComparisonOperator
case object LessOrEqualToOperator    extends ComparisonOperator

/**
  * Aggregations to be used optionally in [[Field]].
  */
sealed trait Aggregation
case object CountAggregation extends Aggregation
case object MaxAggregation   extends Aggregation
case object MinAggregation   extends Aggregation
case object SumAggregation   extends Aggregation

/**
  * Order operators in sql queries. Possible values are [[AscOrderOperator]] or [[DescOrderOperator]].
  */
sealed trait OrderOperator {
  def dimension: String
}
case class AscOrderOperator(override val dimension: String)  extends OrderOperator
case class DescOrderOperator(override val dimension: String) extends OrderOperator

/**
  * Limit operator used to limit the size of search results.
  * @param value the maximum number of results
  */
case class LimitOperator(value: Int)

/**
  * Generic Sql statement.
  * Possible subclasses are: [[SelectSQLStatement]], [[InsertSQLStatement]] or [[DeleteSQLStatement]].
  */
sealed trait SQLStatement extends NSDBStatement {
  def db: String
  def namespace: String
  def metric: String
}

/**
  * Parsed select sql statement case class.
  * @param db the db.
  * @param namespace the namespace.
  * @param metric the metric.
  * @param distinct true if the distinct keyword has been specified in the query.
  * @param fields query fields. See [[SelectedFields]].
  * @param condition the where condition. See [[Condition]].
  * @param groupBy present if the query includes a group by clause.
  * @param order present if the query includes a order clause. See [[OrderOperator]].
  * @param limit present if the query includes a limit clause. See [[LimitOperator]].
  */
case class SelectSQLStatement(override val db: String,
                              override val namespace: String,
                              override val metric: String,
                              distinct: Boolean,
                              fields: SelectedFields,
                              condition: Option[Condition] = None,
                              groupBy: Option[String] = None,
                              order: Option[OrderOperator] = None,
                              limit: Option[LimitOperator] = None)
    extends SQLStatement
    with LazyLogging {

  /**
    * Returns a new instance enriched with a [[RangeExpression]].
    * @param dimension the dimension.
    * @param from the lower boundary of the range expression.
    * @param to the upper boundary of the range expression.
    * @return the enriched instance.
    */
  def enrichWithTimeRange(dimension: String, from: Long, to: Long): SelectSQLStatement = {
    val tsRangeExpression = RangeExpression(dimension, from, to)
    val newCondition = this.condition match {
      case Some(cond) => Condition(TupledLogicalExpression(tsRangeExpression, AndOperator, cond.expression))
      case None       => Condition(tsRangeExpression)
    }
    this.copy(condition = Some(newCondition))
  }

  /**
    * Parses a simple string expression into a [[Expression]] e.g. `dimension > value`.
    * @param dimension the dimension to apply the expression.
    * @param value the expression value.
    * @param operator the operator.
    * @return the parsed [[Expression]].
    */
  private def filterToExpression(dimension: String,
                                 value: Option[JSerializable],
                                 operator: String): Option[Expression] = {
    operator.toUpperCase match {
      case ">"         => Some(ComparisonExpression(dimension, GreaterThanOperator, value.get))
      case ">="        => Some(ComparisonExpression(dimension, GreaterOrEqualToOperator, value.get))
      case "="         => Some(EqualityExpression(dimension, value.get))
      case "<="        => Some(ComparisonExpression(dimension, LessOrEqualToOperator, value.get))
      case "<"         => Some(ComparisonExpression(dimension, LessThanOperator, value.get))
      case "LIKE"      => Some(LikeExpression(dimension, value.get.asInstanceOf[String]))
      case "ISNULL"    => Some(NullableExpression(dimension))
      case "ISNOTNULL" => Some(UnaryLogicalExpression(NullableExpression(dimension), NotOperator))
      case op @ _ =>
        logger.warn("Ignored filter with invalid operator: {}", op)
        None
    }
  }

  /**
    * Returns a new instance enriched with a simple expression got from a string e.g. `dimension > value`.
    * @param filters filters of tuple composed of dimension name, value and operator. See #filterToExpression for more details.
    * @return a new instance of [[SelectSQLStatement]] enriched with the filter provided.
    */
  def addConditions(filters: Seq[(String, Option[JSerializable], String)]): SelectSQLStatement = {
    val expressions: Seq[Expression] = filters.flatMap(f => filterToExpression(f._1, f._2, f._3))
    val filtersExpression =
      expressions.reduce((prevExpr, expr) => TupledLogicalExpression(prevExpr, AndOperator, expr))
    val newCondition: Condition = this.condition match {
      case Some(cond) => Condition(TupledLogicalExpression(cond.expression, AndOperator, filtersExpression))
      case None       => Condition(filtersExpression)
    }
    this.copy(condition = Some(newCondition))
  }

  /**
    * Checks if the current instance has got a time based ordering clause.
    * @return a [[Ordering[Long]] if there is a time base ordering clause.
    */
  def getTimeOrdering: Option[Ordering[Long]] =
    this.order.collect {
      case o: AscOrderOperator if o.dimension == "timestamp"  => implicitly[Ordering[Long]]
      case o: DescOrderOperator if o.dimension == "timestamp" => implicitly[Ordering[Long]].reverse
    }

}

/**
  * Parsed sql insert statement.
  * @param db the db.
  * @param namespace the namespace.
  * @param metric the metric.
  * @param timestamp optional timestamp; if not present the current timestamp will be taken.
  * @param dimensions the dimension to be inserted.
  * @param value the value to be inserted.
  */
case class InsertSQLStatement(override val db: String,
                              override val namespace: String,
                              override val metric: String,
                              timestamp: Option[Long],
                              dimensions: Option[ListAssignment],
                              value: JSerializable)
    extends SQLStatement

/**
  * Parsed delete statement.
  * @param db the db.
  * @param namespace the namespace.
  * @param metric the metric.
  * @param condition the condition to filter records to delete.
  */
case class DeleteSQLStatement(override val db: String,
                              override val namespace: String,
                              override val metric: String,
                              condition: Condition)
    extends SQLStatement

case class DropSQLStatement(override val db: String, override val namespace: String, override val metric: String)
    extends SQLStatement
