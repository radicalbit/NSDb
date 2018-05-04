package io.radicalbit.nsdb.cluster.index

import org.apache.lucene.analysis.standard.StandardAnalyzer
import org.apache.lucene.index.{IndexWriter, IndexWriterConfig}
import org.apache.lucene.store.RAMDirectory
import org.scalatest.{FlatSpec, Matchers, OneInstancePerTest}

class LocationIndexTest extends FlatSpec with Matchers with OneInstancePerTest {

  "MetadataIndex" should "write and read properly" in {

    lazy val directory = new RAMDirectory()

    implicit val writer = new IndexWriter(directory, new IndexWriterConfig(new StandardAnalyzer))

    val metadataIndex = new MetadataIndex(directory)

    (0 to 100).foreach { i =>
      val testData = Location(s"metric_$i", s"node_$i", 0, 0)
      metadataIndex.write(testData)
    }
    writer.close()

    val result = metadataIndex.query(metadataIndex._keyField, "metric_*", Seq.empty, 100)(identity)

    result.size shouldBe 100

    val firstMetadata = metadataIndex.getMetadata("metric_0")

    firstMetadata shouldBe List(
      Location(s"metric_0", s"node_0", 0, 0)
    )
  }

  "MetadataIndex" should "get a single location for a metric" in {

    lazy val directory = new RAMDirectory()

    implicit val writer = new IndexWriter(directory, new IndexWriterConfig(new StandardAnalyzer))

    val metadataIndex = new MetadataIndex(directory)

    (1 to 10).foreach { i =>
      val testData = Location(s"metric_0", s"node_0", i - 1, i)
      metadataIndex.write(testData)
    }
    writer.close()

    val firstMetadata = metadataIndex.getMetadata("metric_0", 1)

    firstMetadata shouldBe Some(
      Location(s"metric_0", s"node_0", 0, 1)
    )

    val intermediateMetadata = metadataIndex.getMetadata("metric_0", 4)

    intermediateMetadata shouldBe Some(
      Location(s"metric_0", s"node_0", 3, 4)
    )

    val lastMetadata = metadataIndex.getMetadata("metric_0", 10)

    lastMetadata shouldBe Some(
      Location(s"metric_0", s"node_0", 9, 10)
    )
  }

}
