package io.radicalbit.nsdb.index

import io.radicalbit.nsdb.index.lucene.AllGroupsAggregationCollector
import io.radicalbit.nsdb.validation.Validation.{FieldValidation, LongValidation}
import org.apache.lucene.analysis.standard.StandardAnalyzer
import org.apache.lucene.document.Field.Store
import org.apache.lucene.document.{Document, LongPoint, StringField}
import org.apache.lucene.index.{DirectoryReader, IndexWriter, IndexWriterConfig}
import org.apache.lucene.queryparser.classic.QueryParser
import org.apache.lucene.search._
import org.apache.lucene.store.BaseDirectory

import scala.collection.mutable.ListBuffer
import scala.util.{Failure, Success, Try}

trait Index[IN, OUT] {
  def directory: BaseDirectory

  def _keyField: String

  def getWriter = new IndexWriter(directory, new IndexWriterConfig(new StandardAnalyzer))

  def getSearcher = new IndexSearcher(DirectoryReader.open(directory))

  def validateRecord(data: IN): FieldValidation
  def toRecord(document: Document): OUT

  protected def write(data: IN)(implicit writer: IndexWriter): LongValidation

  def delete(data: IN)(implicit writer: IndexWriter): Unit

  def delete(query: Query)(implicit writer: IndexWriter): Long = {
    val reader   = DirectoryReader.open(directory)
    val searcher = new IndexSearcher(reader)
    val hits     = searcher.search(query, Int.MaxValue)
    (0 until hits.totalHits).foreach { _ =>
      writer.deleteDocuments(query)
    }
    writer.forceMergeDeletes(true)
    hits.totalHits
  }

  def deleteAll()(implicit writer: IndexWriter): Unit = {
    writer.deleteAll()
    writer.flush()
  }

  private def executeQuery(searcher: IndexSearcher, query: Query, limit: Int, sort: Option[Sort]) = {
    val docs: ListBuffer[Document] = ListBuffer.empty
    val hits =
      sort.fold(searcher.search(query, limit).scoreDocs)(sort => searcher.search(query, limit, sort).scoreDocs)
    (0 until hits.length).foreach { i =>
      val doc = searcher.doc(hits(i).doc)
      docs += doc
    }
    docs.toList
  }

  private[index] def rawQuery(query: Query, limit: Int, sort: Option[Sort]): Seq[Document] = {
    val reader   = DirectoryReader.open(directory)
    val searcher = new IndexSearcher(reader)
    executeQuery(searcher, query, limit, sort)
  }

  private[index] def rawQuery(query: Query, collector: AllGroupsAggregationCollector): Seq[Document] = {
    val reader   = DirectoryReader.open(directory)
    val searcher = new IndexSearcher(reader)
    searcher.search(query, collector)
    collector.getGroupMap.map {
      case (g, v) =>
        val doc = new Document
        doc.add(new StringField(collector.groupField, g, Store.NO))
        doc.add(new LongPoint(collector.aggField, v))
        doc.add(new LongPoint(_keyField, 0))
        doc
    }.toSeq
  }

  def query(query: Query, limit: Int, sort: Option[Sort]): Seq[OUT] = {
    rawQuery(query, limit, sort).map(toRecord)
  }

  def query(query: Query, collector: AllGroupsAggregationCollector): Seq[OUT] = {
    rawQuery(query, collector).map(toRecord)
  }

  def query(field: String, queryString: String, limit: Int, sort: Option[Sort] = None): Seq[OUT] = {
    val reader   = DirectoryReader.open(directory)
    val searcher = new IndexSearcher(reader)
    val parser   = new QueryParser(field, new StandardAnalyzer())
    val query    = parser.parse(queryString)
    executeQuery(searcher, query, limit, sort).map(toRecord)
  }

  def getAll: Seq[OUT] = {
    Try { query(new MatchAllDocsQuery(), Int.MaxValue, None) } match {
      case Success(docs: Seq[OUT]) => docs
      case Failure(_)              => Seq.empty
    }
  }
}
