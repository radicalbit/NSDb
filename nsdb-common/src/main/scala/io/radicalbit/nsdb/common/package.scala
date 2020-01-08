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

/**
  * Contains some useful type aliases.
  */
package object common {

//  type JSerializable = java.io.Serializable
//
//  type JLong   = java.lang.Long
//  type JDouble = java.lang.Double

  sealed trait NSDbType {
    def rawValue: Any

    def concreteManifest: Manifest[_]
  }

  object NSDbType {
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

  sealed trait NSDbNumericType extends NSDbType

  object NSDbNumericType {
    def apply(rawValue: Any): NSDbNumericType = {
      rawValue match {
        case v: Int    => NSDbIntType(v)
        case v: Long   => NSDbLongType(v)
        case v: Double => NSDbDoubleType(v)
        case v =>
          throw new IllegalArgumentException(s"rawValue $v of class ${v.getClass} is not a valid NSDbNumericType")
      }
    }
  }

  case class NSDbIntType(rawValue: Int) extends NSDbNumericType {
    def concreteManifest: Manifest[_] = manifest[Int]
  }
  case class NSDbLongType(rawValue: Long) extends NSDbNumericType {
    def concreteManifest: Manifest[_] = manifest[Long]
  }
  case class NSDbDoubleType(rawValue: Double) extends NSDbNumericType {
    def concreteManifest: Manifest[_] = manifest[Double]
  }
  case class NSDbStringType(rawValue: String) extends NSDbType {
    def concreteManifest: Manifest[_] = manifest[String]
  }

}
