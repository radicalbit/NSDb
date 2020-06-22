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

package io.radicalbit.nsdb

import com.fasterxml.jackson.annotation.{JsonSubTypes, JsonTypeInfo}

/**
  * Contains some useful type aliases.
  */
package object common {

  /**
    * Encapsulates raw types.
    * Direct children (i.e. supported types) are:
    * - [[NSDbNumericType]] encapsulates a numeric type.
    * - [[NSDbStringType]] for String values.
    */
  @JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
  @JsonSubTypes(
    Array(
      new JsonSubTypes.Type(value = classOf[NSDbDoubleType], name = "NSDbDoubleType"),
      new JsonSubTypes.Type(value = classOf[NSDbLongType], name = "NSDbLongType"),
      new JsonSubTypes.Type(value = classOf[NSDbIntType], name = "NSDbIntType"),
      new JsonSubTypes.Type(value = classOf[NSDbStringType], name = "NSDbStringType")
    ))
  sealed trait NSDbType {

    /**
      * @return the raw Value (the Any Type will be specialized in children definition)
      */
    def rawValue: Any

    /**
      * @return the type runtime manifest
      */
    def runtimeManifest: Manifest[_]

    /**
      * convert to a [[NSDbStringType]]
      */
    def toStringType: NSDbStringType = NSDbStringType(rawValue.toString)
  }

  object NSDbType {

    implicit def NSDbTypeLong(value: Long): NSDbType     = NSDbType(value)
    implicit def NSDbTypeInt(value: Int): NSDbType       = NSDbType(value)
    implicit def NSDbTypeDouble(value: Double): NSDbType = NSDbType(value)
    implicit def NSDbTypeString(value: String): NSDbType = NSDbType(value)

    /**
      * Factory method to instantiate a [[NSDbType]] from a supported raw type.
      * @throws IllegalArgumentException if the raw value is not supported.
      */
    @throws[IllegalArgumentException]
    def apply(rawValue: Any): NSDbType = {
      rawValue match {
        case v: Int    => NSDbIntType(v)
        case v: Long   => NSDbLongType(v)
        case v: Double => NSDbDoubleType(v)
        case v: String => NSDbStringType(v)
        case v         => throw new IllegalArgumentException(s"rawValue $v of class ${v.getClass} is not a valid NSDbType")
      }
    }
  }

  /**
    * Encapsulates numeric raw types.
    * Direct children (i.e. supported types) are:
    * - [[NSDbIntType]] for [[Int]] values.
    * - [[NSDbLongType]] for [[Long]] values.
    * - [[NSDbDoubleType]] for [[Double]] values
    */
  @JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
  @JsonSubTypes(
    Array(
      new JsonSubTypes.Type(value = classOf[NSDbDoubleType], name = "NSDbDoubleType"),
      new JsonSubTypes.Type(value = classOf[NSDbLongType], name = "NSDbLongType"),
      new JsonSubTypes.Type(value = classOf[NSDbIntType], name = "NSDbIntType")
    ))
  sealed trait NSDbNumericType extends NSDbType {
    private def compare(other: NSDbNumericType) =
      new java.math.BigDecimal(this.rawValue.toString).compareTo(new java.math.BigDecimal(other.rawValue.toString))

    def >(other: NSDbNumericType): Boolean  = compare(other) == 1
    def >=(other: NSDbNumericType): Boolean = compare(other) == 1 || compare(other) == 0
    def <(other: NSDbNumericType): Boolean  = compare(other) == -1
    def <=(other: NSDbNumericType): Boolean = compare(other) == -1 || compare(other) == 0

    def /(other: NSDbNumericType) = Double.box {
      new java.math.BigDecimal(this.rawValue.toString)
        .divide(new java.math.BigDecimal(other.rawValue.toString))
        .doubleValue()
    }
  }

  object NSDbNumericType {

    implicit def NSDbNumericTypeLong(value: Long): NSDbNumericType     = NSDbNumericType(value)
    implicit def NSDbNumericTypeInt(value: Int): NSDbNumericType       = NSDbNumericType(value)
    implicit def NSDbNumericTypeDouble(value: Double): NSDbNumericType = NSDbNumericType(value)

    /**
      * Factory method to instantiate a [[NSDbNumericType]] from a supported raw type.
      * @throws IllegalArgumentException if the raw value is not supported.
      */
    @throws[IllegalArgumentException]
    def apply(rawValue: Any): NSDbNumericType =
      rawValue match {
        case v: Int    => NSDbIntType(v)
        case v: Long   => NSDbLongType(v)
        case v: Double => NSDbDoubleType(v)
        case v =>
          throw new IllegalArgumentException(s"rawValue $v of class ${v.getClass} is not a valid NSDbNumericType")
      }

    def apply(rawValue: Number): NSDbNumericType =
      rawValue match {
        case x: java.lang.Integer => NSDbNumericType(x.intValue())
        case x: java.lang.Long    => NSDbNumericType(x.longValue())
        case x: java.lang.Float   => NSDbNumericType(x.floatValue().toDouble)
        case x: java.lang.Double  => NSDbNumericType(x.doubleValue())
      }

    def unapply(numeric: NSDbNumericType): Option[Number] = numeric match {
      case NSDbLongType(v)   => Some(v)
      case NSDbIntType(v)    => Some(v)
      case NSDbDoubleType(v) => Some(v)
    }
  }

  case class NSDbIntType(rawValue: Int) extends NSDbNumericType {
    def runtimeManifest: Manifest[_] = manifest[Int]
  }
  case class NSDbLongType(rawValue: Long) extends NSDbNumericType {
    def runtimeManifest: Manifest[_] = manifest[Long]
  }
  case class NSDbDoubleType(rawValue: Double) extends NSDbNumericType {
    def runtimeManifest: Manifest[_] = manifest[Double]
  }
  case class NSDbStringType(rawValue: String) extends NSDbType {
    def runtimeManifest: Manifest[_] = manifest[String]
  }

}
