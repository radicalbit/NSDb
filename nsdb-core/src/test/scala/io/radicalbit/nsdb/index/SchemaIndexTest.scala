package io.radicalbit.nsdb.index

import java.nio.file.Paths
import java.util.UUID

import io.radicalbit.nsdb.model.SchemaField
import org.apache.lucene.analysis.standard.StandardAnalyzer
import org.apache.lucene.index.{IndexWriter, IndexWriterConfig}
import org.apache.lucene.store.RAMDirectory
import org.scalatest.{FlatSpec, Matchers, OneInstancePerTest}

class SchemaIndexTest extends FlatSpec with Matchers with OneInstancePerTest {

  "SchemaIndex" should "write and read properly" in {

    lazy val directory = new RAMDirectory()
    Paths.get(s"target/test_index/SchemaIndex/${UUID.randomUUID}")

    implicit val writer = new IndexWriter(directory, new IndexWriterConfig(new StandardAnalyzer))

    val schemaIndex = new SchemaIndex(directory)

    (0 to 100).foreach { i =>
      val testData = Schema(
        s"metric_$i",
        Seq(SchemaField("field1", BIGINT()), SchemaField("field2", VARCHAR()), SchemaField(s"field$i", VARCHAR())))
      schemaIndex.write(testData)
    }
    writer.close()

    val result = schemaIndex.query(schemaIndex._keyField, "metric_*", Seq.empty, 100)

    result.size shouldBe 100

    val firstSchema = schemaIndex.getSchema("metric_0")

    firstSchema shouldBe Some(
      Schema(
        s"metric_0",
        Seq(SchemaField("field1", BIGINT()), SchemaField("field2", VARCHAR()), SchemaField(s"field0", VARCHAR()))))
  }

  "SchemaIndex" should "update records" in {

    lazy val directory = new RAMDirectory()

    implicit val writer = new IndexWriter(directory, new IndexWriterConfig(new StandardAnalyzer))

    val schemaIndex = new SchemaIndex(directory)

    val testData = Schema(s"metric_1", Seq(SchemaField("field1", BIGINT()), SchemaField("field2", VARCHAR())))
    schemaIndex.write(testData)
    writer.close()

    val result = schemaIndex.getSchema("metric_1")

    result shouldBe Some(testData)

  }

  "SchemaIndex" should "drop schema" in {

    lazy val directory = new RAMDirectory()

    implicit val writer = new IndexWriter(directory, new IndexWriterConfig(new StandardAnalyzer))

    val schemaIndex = new SchemaIndex(directory)

    val testData = Schema(s"metric_2", Seq(SchemaField("field1", BIGINT()), SchemaField("field2", VARCHAR())))
    schemaIndex.write(testData)

    val testDataBis = Schema(s"metric_3", Seq(SchemaField("field1", BIGINT()), SchemaField("field2", VARCHAR())))
    schemaIndex.write(testDataBis)
    writer.close()

    implicit val writerDrop = schemaIndex.getWriter

    val result = schemaIndex.getSchema("metric_2")

    result shouldBe Some(testData)

    schemaIndex.delete(testData)(writerDrop)

    writerDrop.close()

    schemaIndex.getSchema("metric_2") shouldBe None
    schemaIndex.getSchema("metric_3") shouldBe Some(testDataBis)

  }

}
