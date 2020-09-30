package io.radicalbit.nsdb.index.aux

import java.nio.file.Paths
import java.util.UUID

import io.radicalbit.nsdb.common.NSDbStringType
import io.radicalbit.nsdb.common.protocol.Bit
import io.radicalbit.nsdb.index.TimeSeriesIndex
import io.radicalbit.nsdb.model.Schema
import org.apache.lucene.search.MatchAllDocsQuery
import org.apache.lucene.store.MMapDirectory
import org.scalatest.{Matchers, OneInstancePerTest, WordSpec}

class CountDistinctSpec extends WordSpec with Matchers with OneInstancePerTest {

  "TimeSeriesIndex" should {
    "calculate count distinct" in {

      val testRecords: Seq[Bit] = Seq(
        Bit(150000, 2.5, Map("surname" -> "Doe"), Map("name" -> "John")),
        Bit(160000, 2.5, Map("surname" -> "Doe"), Map("name" -> "John")),
        Bit(170000, 2.5, Map("surname" -> "Doe"), Map("name" -> "John")),
        Bit(180000, 2.5, Map("surname" -> "Doe"), Map("name" -> "John")),
        Bit(120000, 3.5, Map("surname" -> "Doe"), Map("name" -> "John")),
        Bit(130000, 3.5, Map("surname" -> "Doe"), Map("name" -> "John")),
        Bit(140000, 3.5, Map("surname" -> "Doe"), Map("name" -> "John")),
        Bit(90000, 5.5, Map("surname" -> "Doe"), Map("name" -> "John")),
        Bit(60000, 7.5, Map("surname" -> "Doe"), Map("name" -> "Bill")),
        Bit(70000, 7.5, Map("surname" -> "Doe"), Map("name" -> "Bill")),
        Bit(80000, 8.5, Map("surname" -> "Doe"), Map("name" -> "Bill")),
        Bit(30000, 4.5, Map("surname" -> "Doe"), Map("name" -> "Frank")),
        Bit(0, 1.5, Map("surname"     -> "Doe"), Map("name" -> "Frankie"))
      )

      val timeSeriesIndex =
        new TimeSeriesIndex(new MMapDirectory(Paths.get("target", "test_first_last_index", UUID.randomUUID().toString)))

      val writer = timeSeriesIndex.getWriter

      testRecords.foreach(timeSeriesIndex.write(_)(writer))

      writer.close()


      val countDistinct = timeSeriesIndex.countDistinct(new MatchAllDocsQuery, Schema("testMetric", testRecords.head), "name")

      countDistinct shouldBe Seq(
        Bit(0,3,Map(),Map("name" -> NSDbStringType("John"))),
        Bit(0,2,Map(),Map("name" -> NSDbStringType("Bill"))),
        Bit(0,1,Map(),Map("name" -> NSDbStringType("Frank"))),
        Bit(0,1,Map(),Map("name" -> NSDbStringType("Frankie")))
      )
    }
  }

}
