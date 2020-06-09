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

import io.radicalbit.nsdb.common.NSDbType
import io.radicalbit.nsdb.common.statement._
import io.radicalbit.nsdb.sql.parser.StatementParserResult.SqlStatementParserSuccess
import org.scalatest.{Matchers, WordSpec}

class InsertSQLStatementSpec extends WordSpec with Matchers {

  private val parser = new SQLStatementParser

  "A SQL parser instance for insert statements" when {

    "receive an insert with a single dimension without tags" should {
      "parse it successfully" in {
        val query = "INSERT INTO people DIM(name=john) VAL=23 "
        parser.parse(db = "db", namespace = "registry", input = query) should be(
          SqlStatementParserSuccess(
            query,
            InsertSQLStatement(
              db = "db",
              namespace = "registry",
              metric = "people",
              timestamp = None,
              dimensions = Some(ListAssignment(Map("name" -> NSDbType("john")))),
              tags = None,
              value = 23L
            )
          )
        )
      }
    }

    "receive an insert with a single tag without dimensions" should {
      "parse it successfully" in {
        val query = "INSERT INTO people TAGS(city='new york') VAL=23 "
        parser.parse(db = "db", namespace = "registry", input = query) should be(
          SqlStatementParserSuccess(
            query,
            InsertSQLStatement(
              db = "db",
              namespace = "registry",
              metric = "people",
              timestamp = None,
              dimensions = None,
              tags = Some(ListAssignment(Map("city" -> NSDbType("new york")))),
              value = 23L
            )
          )
        )
      }
    }

    "receive an insert with a single dimension and a single tag" should {
      "parse it successfully" in {
        val query = "INSERT INTO people DIM(name=john) TAGS(city='new york') VAL=23 "
        parser.parse(db = "db", namespace = "registry", input = query) should be(
          SqlStatementParserSuccess(
            query,
            InsertSQLStatement(
              db = "db",
              namespace = "registry",
              metric = "people",
              timestamp = None,
              dimensions = Some(ListAssignment(Map("name" -> NSDbType("john")))),
              tags = Some(ListAssignment(Map("city"       -> NSDbType("new york")))),
              value = 23L
            )
          )
        )
      }
    }

    "receive an insert with multiple dimensions and a single tag" should {
      "parse it successfully" in {
        val query = "INSERT INTO people DIM(name=john, surname=doe) TAGS(city='new york') VAL=23 "
        parser.parse(db = "db", namespace = "registry", input = query) should be(
          SqlStatementParserSuccess(
            query,
            InsertSQLStatement(
              db = "db",
              namespace = "registry",
              metric = "people",
              timestamp = None,
              dimensions = Some(ListAssignment(Map("name" -> NSDbType("john"), "surname" -> NSDbType("doe")))),
              tags = Some(ListAssignment(Map("city"       -> NSDbType("new york")))),
              value = 23L
            )
          )
        )
      }
    }

    "receive an insert with multiple dimensions, a single tag and a timestamp" should {
      "parse it successfully" in {
        val query = "INSERT INTO people TS=123456 DIM(name=john, surname=doe) TAGS(city='new york') VAL=23 "
        parser.parse(db = "db", namespace = "registry", input = query) should be(
          SqlStatementParserSuccess(
            query,
            InsertSQLStatement(
              db = "db",
              namespace = "registry",
              metric = "people",
              timestamp = Some(123456),
              dimensions = Some(ListAssignment(Map("name" -> NSDbType("john"), "surname" -> NSDbType("doe")))),
              tags = Some(ListAssignment(Map("city"       -> NSDbType("new york")))),
              value = 23L
            )
          )
        )
      }
    }

    "receive an insert with multiple dimensions and tags in int and float format and a timestamp" should {
      "parse it successfully" in {
        val query = "INSERT INTO people TS=123456 DIM(x=1, y=1.5) TAGS(a=3.2, b=4) VAL=23 "
        parser.parse(db = "db", namespace = "registry", input = query) should be(
          SqlStatementParserSuccess(
            query,
            InsertSQLStatement(
              db = "db",
              namespace = "registry",
              metric = "people",
              timestamp = Some(123456),
              dimensions = Some(ListAssignment(Map("x" -> NSDbType(1), "y"   -> NSDbType(1.5)))),
              tags = Some(ListAssignment(Map("a"       -> NSDbType(3.2), "b" -> NSDbType(4)))),
              value = 23L
            )
          )
        )
      }
    }

    "receive an insert with multiple dimensions in int and float format, a fixed tag, a a timestamp and a float value" should {
      "parse it successfully" in {
        val query = "INSERT INTO people TS=123456 DIM(x=1, y=1.5) TAGS(city='new york') VAL=23.5 "
        parser.parse(db = "db", namespace = "registry", input = query) should be(
          SqlStatementParserSuccess(
            query,
            InsertSQLStatement(
              db = "db",
              namespace = "registry",
              metric = "people",
              timestamp = Some(123456),
              dimensions = Some(ListAssignment(Map("x" -> NSDbType(1), "y" -> NSDbType(1.5)))),
              tags = Some(ListAssignment(Map("city"    -> NSDbType("new york")))),
              value = 23.5
            )
          )
        )
      }
    }

    "receive a insert metric without dimensions, tags and timestamp" should {
      "succeed" in {
        val query = "INSERT INTO people val=23 "
        parser.parse(db = "db", namespace = "registry", input = query) should be(
          SqlStatementParserSuccess(query,
                                    InsertSQLStatement(db = "db",
                                                       namespace = "registry",
                                                       metric = "people",
                                                       timestamp = None,
                                                       dimensions = None,
                                                       tags = None,
                                                       value = 23L))
        )
      }
    }

    "receive a insert metric with dimension string with spaces and tags without spaces" should {
      "succeed" in {
        val query = "INSERT INTO people DIM(name = 'spaced string') TAGS(city = chicago) val=23 "
        parser.parse(db = "db", namespace = "registry", input = query) should be(
          SqlStatementParserSuccess(
            query,
            InsertSQLStatement(
              db = "db",
              namespace = "registry",
              metric = "people",
              timestamp = None,
              dimensions = Some(ListAssignment(Map("name" -> NSDbType("spaced string")))),
              tags = Some(ListAssignment(Map("city"       -> NSDbType("chicago")))),
              23L
            )
          )
        )
      }
    }

    "receive a insert metric with dimension string with one char" should {
      "succeed" in {
        val query = "INSERT INTO people DIM(name = 'a') val=23 "
        parser.parse(db = "db", namespace = "registry", input = query) should be(
          SqlStatementParserSuccess(query,
                                    InsertSQLStatement(db = "db",
                                                       "registry",
                                                       "people",
                                                       None,
                                                       Some(ListAssignment(Map("name" -> NSDbType("a")))),
                                                       tags = None,
                                                       23L))
        )
      }
    }
  }
}
