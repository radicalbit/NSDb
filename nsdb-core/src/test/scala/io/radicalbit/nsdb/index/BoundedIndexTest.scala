package io.radicalbit.nsdb.index

import java.nio.file.Paths
import java.util.UUID

import io.radicalbit.nsdb.model.Record
import org.apache.lucene.analysis.standard.StandardAnalyzer
import org.apache.lucene.document.LongPoint
import org.apache.lucene.index.{IndexWriter, IndexWriterConfig}
import org.apache.lucene.search.{Sort, SortField}
import org.apache.lucene.store.FSDirectory
import org.scalatest.{FlatSpec, Matchers, OneInstancePerTest}

class BoundedIndexTest extends FlatSpec with Matchers with OneInstancePerTest {

  "BoundedIndex" should "write and read properly on disk" in {

    lazy val directory = FSDirectory.open(Paths.get(s"target/test_index/${UUID.randomUUID}"))

    implicit val writer = new IndexWriter(directory, new IndexWriterConfig(new StandardAnalyzer))

    val boundedIndex = new BoundedIndex(directory)

    (0 to 100).foreach { i =>
      val testData = Record(System.currentTimeMillis, Map("content" -> s"content_$i"), Map.empty)
      boundedIndex.write(testData)
    }
    writer.close()

    val result = boundedIndex.query("content", "content_*", 100)

    result.size shouldBe 100

  }

  "BoundedIndex" should "support range queries and sorting" in {
    lazy val directory = FSDirectory.open(Paths.get(s"target/test_index/${UUID.randomUUID}"))

    implicit val writer = new IndexWriter(directory, new IndexWriterConfig(new StandardAnalyzer))

    val boundedIndex = new BoundedIndex(directory)

    (0 to 100).foreach { i =>
      val testData = Record(System.currentTimeMillis, Map("content" -> s"content_$i"), Map.empty)
      boundedIndex.write(testData)
    }

    writer.close()

    val query = LongPoint.newRangeQuery("_lastRead", 0, Long.MaxValue)

    val result = boundedIndex.rawQuery(query, 100, Some(new Sort(new SortField("_lastRead", SortField.Type.DOC))))

    result.size shouldBe 100

    (1 to 99).foreach { i =>
      result(i).getField("_lastRead").numericValue().longValue should be >= result(i - 1)
        .getField("_lastRead")
        .numericValue
        .longValue
    }
  }

  "BoundedIndex" should "delete records" in {
    implicit lazy val directory = FSDirectory.open(Paths.get(s"target/test_index/${UUID.randomUUID}"))

    implicit val writer = new IndexWriter(directory, new IndexWriterConfig(new StandardAnalyzer))

    val boundedIndex = new BoundedIndex(directory)

    val timestamp = System.currentTimeMillis

    val testData = Record(timestamp, Map("content" -> s"content"), Map.empty)

    boundedIndex.write(testData)

    writer.flush()
    writer.close()

    val queryExist = LongPoint.newRangeQuery("_timestamp", timestamp, timestamp)
    val resultExist =
      boundedIndex.query(queryExist, 100, Some(new Sort(new SortField("_lastRead", SortField.Type.DOC))))
    resultExist.size shouldBe 1

    val deleteWriter = new IndexWriter(directory, new IndexWriterConfig(new StandardAnalyzer))
    boundedIndex.delete(testData)(deleteWriter)

    deleteWriter.flush()
    deleteWriter.close()

    val query  = LongPoint.newRangeQuery("_timestamp", timestamp, timestamp)
    val result = boundedIndex.query(query, 100, Some(new Sort(new SortField("_lastRead", SortField.Type.DOC))))

    result.size shouldBe 0

  }
}
