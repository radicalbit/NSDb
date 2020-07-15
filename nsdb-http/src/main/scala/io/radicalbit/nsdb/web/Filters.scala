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

package io.radicalbit.nsdb.web

import com.fasterxml.jackson.annotation.{JsonSubTypes, JsonTypeInfo}
import io.radicalbit.nsdb.common.NSDbType
import io.radicalbit.nsdb.common.protocol.NSDbSerializable
import io.radicalbit.nsdb.protocol.MessageProtocol.Events.Generic
import io.swagger.annotations.{ApiModel, ApiModelProperty}

import scala.annotation.meta.field

object Filters {

  @ApiModel(description = "Filter Operators enumeration with [=, >, >=, <, <=, LIKE]")
  object FilterOperators extends Enumeration {
    val Equality       = Value("=")
    val GreaterThan    = Value(">")
    val GreaterOrEqual = Value(">=")
    val LessThan       = Value("<")
    val LessOrEqual    = Value("<=")
    val Like           = Value("LIKE")
  }

  @ApiModel(description = "Filter Nullability Operators enumeration with [ISNULL, ISNOTNULL]")
  object NullableOperators extends Enumeration {
    val IsNull    = Value("ISNULL")
    val IsNotNull = Value("ISNOTNULL")
  }

  @ApiModel(description = "Filter sealed trait", subTypes = Array(classOf[FilterNullableValue], classOf[FilterByValue]))
  @JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
  @JsonSubTypes(
    Array(
      new JsonSubTypes.Type(value = classOf[FilterByValue], name = "FilterByValue"),
      new JsonSubTypes.Type(value = classOf[Generic], name = "FilterNullableValue")
    ))
  sealed trait Filter extends NSDbSerializable

  case object Filter {
    def unapply(arg: Filter): Option[(String, Option[NSDbType], String)] =
      arg match {
        case byValue: FilterByValue => Some((byValue.dimension, Some(byValue.value), byValue.operator.toString))
        case nullableValue: FilterNullableValue =>
          Some((nullableValue.dimension, None, nullableValue.operator.toString))
      }
  }

  @ApiModel(description = "Filter using operator", parent = classOf[Filter])
  case class FilterByValue(
      @(ApiModelProperty @field)(value = "dimension on which apply condition") dimension: String,
      @(ApiModelProperty @field)(value = "value of comparation") value: NSDbType,
      @(ApiModelProperty @field)(
        value = "filter comparison operator",
        dataType = "io.radicalbit.nsdb.web.routes.FilterOperators") operator: FilterOperators.Value
  ) extends Filter

  @ApiModel(description = "Filter for nullable", parent = classOf[Filter])
  case class FilterNullableValue(
      @(ApiModelProperty @field)(value = "dimension on which apply condition") dimension: String,
      @(ApiModelProperty @field)(
        value = "filter nullability operator",
        dataType = "io.radicalbit.nsdb.web.routes.NullableOperators") operator: NullableOperators.Value
  ) extends Filter
}
