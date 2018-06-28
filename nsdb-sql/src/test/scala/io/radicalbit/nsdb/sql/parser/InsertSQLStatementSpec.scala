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

import io.radicalbit.nsdb.common.statement.{InsertSQLStatement, ListAssignment}
import org.scalatest.{Matchers, WordSpec}

import scala.util.Success

class InsertSQLStatementSpec extends WordSpec with Matchers {

  private val parser = new SQLStatementParser

  "A parser instance for insert statements" when {

    "receive an insert with a single dimension without tags" should {
      "parse it successfully" in {
        parser.parse(db = "db", namespace = "registry", input = "INSERT INTO people DIM(name=john) VAL=23 ") should be(
          Success(
            InsertSQLStatement(db = "db",
                               namespace = "registry",
                               metric = "people",
                               timestamp = None,
                               dimensions = Some(ListAssignment(Map("name" -> "john"))),
                               tags = None,
                               value = 23))
        )
      }
    }

    "receive an insert with a single tag without dimensions" should {
      "parse it successfully" in {
        parser.parse(db = "db", namespace = "registry", input = "INSERT INTO people TAGS(city='new york') VAL=23 ") should be(
          Success(
            InsertSQLStatement(db = "db",
                               namespace = "registry",
                               metric = "people",
                               timestamp = None,
                               dimensions = None,
                               tags = Some(ListAssignment(Map("city" -> "new york"))),
                               value = 23))
        )
      }
    }

    "receive an insert with a single dimension and a single tag" should {
      "parse it successfully" in {
        parser.parse(db = "db",
                     namespace = "registry",
                     input = "INSERT INTO people DIM(name=john) TAGS(city='new york') VAL=23 ") should be(
          Success(
            InsertSQLStatement(
              db = "db",
              namespace = "registry",
              metric = "people",
              timestamp = None,
              dimensions = Some(ListAssignment(Map("name" -> "john"))),
              tags = Some(ListAssignment(Map("city"       -> "new york"))),
              value = 23
            ))
        )
      }
    }

    "receive an insert with multiple dimensions and a single tag" should {
      "parse it successfully" in {
        parser.parse(db = "db",
                     namespace = "registry",
                     input = "INSERT INTO people DIM(name=john, surname=doe) TAGS(city='new york') VAL=23 ") should be(
          Success(
            InsertSQLStatement(
              db = "db",
              namespace = "registry",
              metric = "people",
              timestamp = None,
              dimensions = Some(ListAssignment(Map("name" -> "john", "surname" -> "doe"))),
              tags = Some(ListAssignment(Map("city"       -> "new york"))),
              value = 23
            ))
        )
      }
    }

    "receive an insert with multiple dimensions, a single tag and a timestamp" should {
      "parse it successfully" in {
        parser.parse(
          db = "db",
          namespace = "registry",
          input = "INSERT INTO people TS=123456 DIM(name=john, surname=doe) TAGS(city='new york') VAL=23 ") should be(
          Success(
            InsertSQLStatement(
              db = "db",
              namespace = "registry",
              metric = "people",
              timestamp = Some(123456),
              dimensions = Some(ListAssignment(Map("name" -> "john", "surname" -> "doe"))),
              tags = Some(ListAssignment(Map("city"       -> "new york"))),
              value = 23
            ))
        )
      }
    }

    "receive an insert with multiple dimensions and tags in int and float format and a timestamp" should {
      "parse it successfully" in {
        parser.parse(db = "db",
                     namespace = "registry",
                     input = "INSERT INTO people TS=123456 DIM(x=1, y=1.5) TAGS(a=3.2, b=4) VAL=23 ") should be(
          Success(
            InsertSQLStatement(
              db = "db",
              namespace = "registry",
              metric = "people",
              timestamp = Some(123456),
              dimensions = Some(ListAssignment(Map("x" -> 1, "y"   -> 1.5))),
              tags = Some(ListAssignment(Map("a"       -> 3.2, "b" -> 4))),
              value = 23
            ))
        )
      }
    }

    "receive an insert with multiple dimensions in int and float format, a fixed tag, a a timestamp and a float value" should {
      "parse it successfully" in {
        parser.parse(db = "db",
                     namespace = "registry",
                     input = "INSERT INTO people TS=123456 DIM(x=1, y=1.5) TAGS(city='new york') VAL=23.5 ") should be(
          Success(
            InsertSQLStatement(
              db = "db",
              namespace = "registry",
              metric = "people",
              timestamp = Some(123456),
              dimensions = Some(ListAssignment(Map("x" -> 1, "y" -> 1.5))),
              tags = Some(ListAssignment(Map("city"    -> "new york"))),
              value = 23.5
            ))
        )
      }
    }

    "receive a insert metric without dimensions, tags and timestamp" should {
      "succeed" in {
        parser.parse(db = "db", namespace = "registry", input = "INSERT INTO people val=23) ") should be(
          Success(
            InsertSQLStatement(db = "db",
                               namespace = "registry",
                               metric = "people",
                               timestamp = None,
                               dimensions = None,
                               tags = None,
                               value = 23)
          )
        )
      }
    }

    "receive a insert metric with dimension string with spaces and tags without spaces" should {
      "succeed" in {
        parser.parse(db = "db",
                     namespace = "registry",
                     input = "INSERT INTO people DIM(name = 'spaced string') TAGS(city = chicago) val=23) ") should be(
          Success(
            InsertSQLStatement(
              db = "db",
              namespace = "registry",
              metric = "people",
              timestamp = None,
              dimensions = Some(ListAssignment(Map("name" -> "spaced string"))),
              tags = Some(ListAssignment(Map("city"       -> "chicago"))),
              23
            )
          )
        )
      }
    }

    "receive a insert metric with dimension string with one character" should {
      "succeed" in {
        parser.parse(db = "db", namespace = "registry", input = "INSERT INTO people DIM(name = 'a') val=23 ") should be(
          Success(
            InsertSQLStatement(db = "db",
                               "registry",
                               "people",
                               None,
                               Some(ListAssignment(Map("name" -> "a"))),
                               tags = None,
                               23)
          )
        )
      }
    }
  }
}
