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
