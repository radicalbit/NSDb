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

package io.radicalbit.nsdb.connector.kafka.sink

import org.scalatest.{FlatSpec, Matchers, OneInstancePerTest}

class ParsedKcqlSpec extends FlatSpec with Matchers with OneInstancePerTest {

  "KcqlFields" should "refuse to convert invalid kcqls" in {

    an[IllegalArgumentException] should be thrownBy ParsedKcql(
      "INSERT INTO metric SELECT x AS a, y AS b, z AS c FROM topic",
      None,
      None)
    an[IllegalArgumentException] should be thrownBy ParsedKcql(
      "INSERT INTO metric SELECT x AS a, y AS b, z AS c FROM topic WITHTIMESTAMP y",
      None,
      None)
    an[IllegalArgumentException] should be thrownBy ParsedKcql(
      "INSERT INTO metric SELECT x AS db, y AS b, z AS c FROM topic WITHTIMESTAMP y",
      None,
      None)
    an[IllegalArgumentException] should be thrownBy ParsedKcql(
      "INSERT INTO metric SELECT x AS db, y AS namespace, z AS c FROM topic WITHTIMESTAMP y",
      None,
      None)

    ParsedKcql("INSERT INTO metric SELECT x AS db, y AS namespace, z AS value FROM topic WITHTIMESTAMP y", None, None) shouldBe ParsedKcql(
      "x",
      "y",
      "metric",
      Map("value" -> "z", "timestamp" -> "y"))
  }

  "KcqlFields" should "accept kcqls without db and namespace mapping but with global configs instead" in {
    ParsedKcql("INSERT INTO metric SELECT z AS value FROM topic WITHTIMESTAMP y", Some("x"), Some("y")) shouldBe ParsedKcql(
      "x",
      "y",
      "metric",
      Map("value" -> "z", "timestamp" -> "y"))
  }

  "KcqlFields" should "successfully convert queries with or without aliases" in {
    val noDimensionQuery = "INSERT INTO metric SELECT x AS db, y AS namespace, z AS value FROM topic WITHTIMESTAMP y"

    ParsedKcql(noDimensionQuery, None, None) shouldBe ParsedKcql("x",
                                                                 "y",
                                                                 "metric",
                                                                 Map("value" -> "z", "timestamp" -> "y"))

    val withDimensionNoAlias =
      "INSERT INTO metric SELECT x AS db, y AS namespace, z AS value, d FROM topic WITHTIMESTAMP t"

    ParsedKcql(withDimensionNoAlias, None, None) shouldBe ParsedKcql(
      "x",
      "y",
      "metric",
      Map("value" -> "z", "timestamp" -> "t", "d" -> "d"))

    val withDimensionAlias =
      "INSERT INTO metric SELECT x AS db, y AS namespace, z AS value, d as dimension FROM topic WITHTIMESTAMP y"

    ParsedKcql(withDimensionAlias, None, None) shouldBe ParsedKcql(
      "x",
      "y",
      "metric",
      Map("value" -> "z", "timestamp" -> "y", "dimension" -> "d"))
  }

}
