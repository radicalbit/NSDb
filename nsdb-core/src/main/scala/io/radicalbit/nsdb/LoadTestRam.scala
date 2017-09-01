package io.radicalbit.nsdb

import java.nio.file.Paths
import java.util.UUID

import io.radicalbit.nsdb.common.protocol.Bit
import io.radicalbit.nsdb.index.TimeSeriesIndex
import org.apache.lucene.analysis.standard.StandardAnalyzer
import org.apache.lucene.index.{IndexWriter, IndexWriterConfig}
import org.apache.lucene.store.FSDirectory

object LoadTestRam extends App {

  lazy val ramDisk = FSDirectory.open(Paths.get(s"/Volumes/ramdisk/test_index/${UUID.randomUUID}"))

  val ramWriter = new IndexWriter(ramDisk, new IndexWriterConfig(new StandardAnalyzer))

  val ramBoundedIndex = new TimeSeriesIndex(ramDisk)

  val startRam = System.currentTimeMillis

  (0 to 10000000).foreach { i =>
    val testData = Bit(timestamp = System.currentTimeMillis, value = 0, dimensions = Map("content" -> s"content_$i"))
    ramBoundedIndex.write(testData)(ramWriter)
  }

  ramWriter.close

  var result = ramBoundedIndex.query("content", "content_*", Seq.empty, 100)

  println(s"ram end in ${System.currentTimeMillis - startRam}")
}
