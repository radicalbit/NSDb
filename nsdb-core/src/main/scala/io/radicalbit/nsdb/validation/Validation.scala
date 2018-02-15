package io.radicalbit.nsdb.validation

import cats.Monoid
import cats.data.Validated.{Invalid, Valid, valid}
import cats.data.{NonEmptyList, Validated}
import io.radicalbit.nsdb.model.SchemaField
import org.apache.lucene.document.Field
import cats.implicits._
import cats.kernel.Semigroup

object Validation {
//  type FieldValidation  = Validated[NonEmptyList[String], Seq[Field]]
//  type WriteValidation  = Validated[NonEmptyList[String], Long]
  type SchemaValidation = Validated[NonEmptyList[String], Seq[SchemaField]]

  implicit val fieldSemigroup = new Semigroup[Seq[Field]] {
    def combine(first: Seq[Field], second: Seq[Field]): Seq[Field] = first ++ second
  }

  implicit val schemaValidationMonoid = new Monoid[SchemaValidation] {
    override val empty: SchemaValidation = valid(Seq.empty)

    override def combine(x: SchemaValidation, y: SchemaValidation): SchemaValidation =
      (x, y) match {
        case (Valid(a), Valid(b))       => valid(a ++ b)
        case (Valid(_), k @ Invalid(_)) => k
        case (f @ Invalid(_), Valid(_)) => f
        case (Invalid(l1), Invalid(l2)) => Invalid(l1.combine(l2))
      }
  }
}
