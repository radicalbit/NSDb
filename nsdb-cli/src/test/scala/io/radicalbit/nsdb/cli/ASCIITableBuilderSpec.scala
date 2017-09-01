package io.radicalbit.nsdb.cli

import io.radicalbit.nsdb.common.protocol.{BitOut, SQLStatementExecuted}
import org.scalatest.{Matchers, WordSpec}

import scala.util.Success

class ASCIITableBuilderSpec extends WordSpec with Matchers {

  def statementFor(res: Seq[BitOut]) = SQLStatementExecuted(namespace = "registry", metric = "people", res)

  "A parser instance" when {

    "receive a select projecting a wildcard" should {
      "parse it successfully" in {

        val input = List(
          BitOut(timestamp = 1L,
                 value = 10,
                 dimensions = Map(("name"                                -> "Roger"), ("surname" -> "Sterling"), ("age" -> 65))),
          BitOut(timestamp = 2L, value = 20, dimensions = Map(("name"    -> "Don"), ("surname" -> "Draper"))),
          BitOut(timestamp = 3L, value = 30, dimensions = Map(("age"     -> 28), ("surname" -> "Olson"))),
          BitOut(timestamp = 4L, value = 40, dimensions = Map(("name"    -> "Pete"))),
          BitOut(timestamp = 5L, value = 50, dimensions = Map(("age"     -> "32"))),
          BitOut(timestamp = 6L, value = 60, dimensions = Map(("surname" -> "Holloway")))
        )

        val expected = Success(
          "+---------------+---------------+---------------+---------------+--------------+\n|timestamp      |value          |age            |name           |surname       |\n+---------------+---------------+---------------+---------------+--------------+\n|1              |10             |65             |Roger          |Sterling      |\n+---------------+---------------+---------------+---------------+--------------+\n|2              |20             |               |Don            |Draper        |\n+---------------+---------------+---------------+---------------+--------------+\n|3              |30             |28             |               |Olson         |\n+---------------+---------------+---------------+---------------+--------------+\n|4              |40             |               |Pete           |              |\n+---------------+---------------+---------------+---------------+--------------+\n|5              |50             |32             |               |              |\n+---------------+---------------+---------------+---------------+--------------+\n|6              |60             |               |               |Holloway      |\n+---------------+---------------+---------------+---------------+--------------+")

        ASCIITableBuilder.tableFor(statementFor(input)) shouldBe expected
      }
    }
  }
}
