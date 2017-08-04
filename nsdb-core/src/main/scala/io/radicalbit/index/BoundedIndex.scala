package io.radicalbit.index

import io.radicalbit.model.Record
import org.apache.lucene.document.{Document, LongPoint, StoredField}
import org.apache.lucene.index.{DirectoryReader, IndexWriter}
import org.apache.lucene.search.{IndexSearcher, Sort, SortField}
import org.apache.lucene.store.BaseDirectory

import scala.util.{Failure, Success, Try}

class BoundedIndex(override val directory: BaseDirectory) extends TimeSeriesIndex[Record] with TypeSupport {

  private val nRetry = 10

  override def writeRecord(doc: Document, data: Record): Try[Document] = {
    validateSchema(data.dimensions ++ data.fields) match {
      case Success(_) =>
        for (elem: (String, Any) <- data.dimensions) {
          val indexField = IndexType.fromClass(elem._2.getClass).get
          indexField.indexField(elem._1, elem._2).foreach(doc.add(_))
        }
        for (elem: (String, Any) <- data.fields) {
          doc.add(new StoredField(elem._1, elem._2.toString))
        }
        Success(doc)
      case Failure(ex) => Failure(ex)
    }
  }

  def write(data: Record, attempt: Int = 0)(implicit writer: IndexWriter): Try[Long] = {
    write(data)(writer) match {
      case Success(id) => Try(id)
      case Failure(_: OutOfMemoryError) if attempt < nRetry =>
        val reader   = DirectoryReader.open(directory)
        val searcher = new IndexSearcher(reader)
        writer.deleteUnusedFiles()
        val query = LongPoint.newRangeQuery(_lastRead, 0, Long.MaxValue)
        val hits  = searcher.search(query, attempt, new Sort(new SortField(_lastRead, SortField.Type.DOC)))
        (1 until hits.totalHits).foreach { i =>
          writer.tryDeleteDocument(reader, hits.scoreDocs(i).doc)
        }
        writer.forceMergeDeletes()
        write(data, attempt + 1)
      case Failure(ex: OutOfMemoryError) if attempt == nRetry => Failure(ex)
      case Failure(ex)                                        => Failure(ex)
    }
  }

  def deleteAll()(implicit writer: IndexWriter): Unit = {
    writer.deleteAll()
    writer.flush()
  }

}

object BoundedIndex {}
