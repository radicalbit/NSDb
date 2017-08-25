package io.radicalbit.nsdb.index

import io.radicalbit.nsdb.index.lucene.AllGroupsAggregationCollector
import io.radicalbit.nsdb.validation.Validation.{FieldValidation, LongValidation}
import org.apache.lucene.analysis.standard.StandardAnalyzer
import org.apache.lucene.document.Document
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

  private def executeAggregateQuery(searcher: IndexSearcher,
                                    query: Query,
                                    collector: AllGroupsAggregationCollector): Map[String, Long] = {
    searcher.search(query, collector)
    collector.getGroupMap
  }

  private[index] def rawQuery(query: Query, limit: Int, sort: Option[Sort]): Seq[Document] = {
    val reader   = DirectoryReader.open(directory)
    val searcher = new IndexSearcher(reader)
    executeQuery(searcher, query, limit, sort)
  }

  private[index] def rawAggregateQuery(query: Query, collector: AllGroupsAggregationCollector): Map[String, Long] = {
    val reader   = DirectoryReader.open(directory)
    val searcher = new IndexSearcher(reader)
    executeAggregateQuery(searcher, query, collector)
  }

  def query(query: Query, limit: Int, sort: Option[Sort]): Seq[OUT] = {
    rawQuery(query, limit, sort).map(toRecord)
  }

  def aggregateQuery(query: Query, collector: AllGroupsAggregationCollector): Map[String, Long] = {
    rawAggregateQuery(query, collector)
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
