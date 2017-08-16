package io.radicalbit.nsdb.index

import org.apache.lucene.document.{Document, LongPoint, StoredField}
import org.apache.lucene.index.{DirectoryReader, IndexWriter}
import org.apache.lucene.search.{IndexSearcher, Sort}

import scala.util.Try

trait TimeSerieRecord {
  val timestamp: Long
}

trait TimeSeriesIndex[RECORD <: TimeSerieRecord] extends Index[RECORD] {

  val _lastRead          = "_lastRead"
  override val _keyField = "_timestamp"

  def writeRecord(doc: Document, data: RECORD): Try[Document]

  protected def write(data: RECORD)(implicit writer: IndexWriter): Try[Long] = {
    val doc = new Document
    doc.add(new LongPoint(_keyField, data.timestamp))
    doc.add(new StoredField(_keyField, data.timestamp))
    writeRecord(doc, data)
    val curTime = System.currentTimeMillis
    doc.add(new LongPoint(_lastRead, curTime))
    doc.add(new StoredField(_lastRead, System.currentTimeMillis))
    Try { writer.addDocument(doc) }
  }

  def delete(data: RECORD)(implicit writer: IndexWriter): Unit = {
    val query    = LongPoint.newRangeQuery(_keyField, data.timestamp, data.timestamp)
    val reader   = DirectoryReader.open(directory)
    val searcher = new IndexSearcher(reader)
    val hits     = searcher.search(query, 1)
    (0 until hits.totalHits).foreach { i =>
      writer.deleteDocuments(query)
    }
    writer.forceMergeDeletes(true)
  }

  def timeRange(start: Long, end: Long, size: Int = Int.MaxValue, sort: Option[Sort] = None) = {
    val rangeQuery = LongPoint.newRangeQuery(_keyField, start, end)
    query(rangeQuery, size, sort)
  }

}
