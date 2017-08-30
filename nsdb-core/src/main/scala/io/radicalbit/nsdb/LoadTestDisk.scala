package io.radicalbit.nsdb

import java.nio.file.Paths
import java.util.UUID

import io.radicalbit.nsdb.common.protocol.Record
import io.radicalbit.nsdb.index.TimeSeriesIndex
import org.apache.lucene.analysis.standard.StandardAnalyzer
import org.apache.lucene.index.{IndexWriter, IndexWriterConfig}
import org.apache.lucene.store.FSDirectory

object LoadTestDisk extends App {

  lazy val disk = FSDirectory.open(Paths.get(s"target/test_index/${UUID.randomUUID}"))

  val diskWriter = new IndexWriter(disk, new IndexWriterConfig(new StandardAnalyzer))

  val diskBoundedIndex = new TimeSeriesIndex(disk)

  val startDisk = System.currentTimeMillis

  (0 to 10000000).foreach { i =>
    val testData = Record(System.currentTimeMillis, Map("content" -> s"content_$i"), 0)
    diskBoundedIndex.write(testData)(diskWriter)
  }

  diskWriter.close

  var result = diskBoundedIndex.query("content", "content_*", 100)

  println(s"disk end in ${System.currentTimeMillis - startDisk}")

}
