package io.radicalbit.nsdb.index

import java.nio.file.Paths
import java.util.UUID

import io.radicalbit.nsdb.common.protocol.Bit
import io.radicalbit.nsdb.index.lucene.MaxAllGroupsCollector
import org.apache.lucene.analysis.standard.StandardAnalyzer
import org.apache.lucene.document.LongPoint
import org.apache.lucene.index.{IndexWriter, IndexWriterConfig}
import org.apache.lucene.search.{MatchAllDocsQuery, Sort, SortField}
import org.apache.lucene.store.NIOFSDirectory
import org.scalatest.{FlatSpec, Matchers, OneInstancePerTest}

class TimeSeriesIndexTest extends FlatSpec with Matchers with OneInstancePerTest {

  "TimeSeriesIndex" should "write and read properly on disk" in {

    lazy val directory = new NIOFSDirectory(Paths.get(s"target/test_index/${UUID.randomUUID}"))

    implicit val writer = new IndexWriter(directory, new IndexWriterConfig(new StandardAnalyzer))

    val boundedIndex = new TimeSeriesIndex(directory)

    (0 to 100).foreach { i =>
      val testData =
        Bit(timestamp = System.currentTimeMillis, value = 23, dimensions = Map("content" -> s"content_$i"))
      boundedIndex.write(testData)
    }
    writer.close()

    val result = boundedIndex.query("content", "content_*", Seq.empty, 100)

    result.size shouldBe 100

  }

  "TimeSeriesIndex" should "support range queries and sorting" in {
    lazy val directory = new NIOFSDirectory(Paths.get(s"target/test_index/${UUID.randomUUID}"))

    implicit val writer = new IndexWriter(directory, new IndexWriterConfig(new StandardAnalyzer))

    val boundedIndex = new TimeSeriesIndex(directory)

    (0 to 100).foreach { i =>
      val testData =
        Bit(timestamp = System.currentTimeMillis, value = 23.5, dimensions = Map("content" -> s"content_$i"))
      boundedIndex.write(testData)
    }

    writer.close()

    val query = new MatchAllDocsQuery()

    val result = boundedIndex.rawQuery(query, 100, Some(new Sort(new SortField("timestamp", SortField.Type.DOC))))

    result.size shouldBe 100

    (1 to 99).foreach { i =>
      result(i).getField("timestamp").numericValue().longValue should be >= result(i - 1)
        .getField("timestamp")
        .numericValue
        .longValue
    }
  }

  "TimeSeriesIndex" should "delete records" in {
    implicit lazy val directory = new NIOFSDirectory(Paths.get(s"target/test_index/${UUID.randomUUID}"))

    implicit val writer = new IndexWriter(directory, new IndexWriterConfig(new StandardAnalyzer))

    val boundedIndex = new TimeSeriesIndex(directory)

    val timestamp = System.currentTimeMillis

    val testData = Bit(timestamp = timestamp, value = 0.2, dimensions = Map("content" -> s"content"))

    boundedIndex.write(testData)

    writer.flush()
    writer.close()

    val queryExist = LongPoint.newRangeQuery("timestamp", timestamp, timestamp)
    val resultExist =
      boundedIndex.query(queryExist, Seq.empty, 100, None)
    resultExist.size shouldBe 1

    val deleteWriter = new IndexWriter(directory, new IndexWriterConfig(new StandardAnalyzer))
    boundedIndex.delete(testData)(deleteWriter)

    deleteWriter.flush()
    deleteWriter.close()

    val query = LongPoint.newRangeQuery("timestamp", timestamp, timestamp)
    val result =
      boundedIndex.query(query, Seq.empty, 100, None)

    result.size shouldBe 0

  }

  "TimeSeriesIndex" should "support groupBy queries" in {
    lazy val directory = new NIOFSDirectory(Paths.get(s"target/test_index/${UUID.randomUUID}"))

    implicit val writer = new IndexWriter(directory, new IndexWriterConfig(new StandardAnalyzer))

    val boundedIndex = new TimeSeriesIndex(directory)

    (0 to 9).foreach { i =>
      val testData = Bit(timestamp = System.currentTimeMillis,
                         value = 10,
                         dimensions = Map("content" -> s"content_${i / 4}", "number" -> i))
      boundedIndex.write(testData)
    }

    writer.close()

    val collector = new MaxAllGroupsCollector("content", "number")

    boundedIndex.getSearcher.search(new MatchAllDocsQuery(), collector)

    collector.getGroupCount shouldBe 3
    val sorted = collector.getGroupMap.toSeq.sortBy(_._2)
    sorted shouldBe Seq(("content_0", 3), ("content_1", 7), ("content_2", 9))
  }
}
