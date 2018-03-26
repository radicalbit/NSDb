package io.radicalbit.nsdb.index

import io.radicalbit.nsdb.index.lucene.AllGroupsAggregationCollector
import io.radicalbit.nsdb.statement.StatementParser.SimpleField
import org.apache.lucene.analysis.standard.StandardAnalyzer
import org.apache.lucene.document._
import org.apache.lucene.index.{IndexWriter, IndexWriterConfig}
import org.apache.lucene.queryparser.classic.QueryParser
import org.apache.lucene.search._
import org.apache.lucene.store.BaseDirectory

import scala.collection.mutable.ListBuffer
import scala.util.{Failure, Success, Try}

/**
  * Trait for a generic lucene index.
  * @tparam T the entity read and written in the index.
  */
trait Index[T] {

  /**
    * @return index base directory.
    */
  def directory: BaseDirectory

  /**
    * @return index entry identifier.
    */
  def _keyField: String

  /**
    * number of occurrences
    */
  val _countField: String = "_count"

  val _valueField = "value"

  private lazy val searcherManager: SearcherManager = new SearcherManager(directory, null)

  /**
    * @return a lucene [[IndexWriter]] to be used in write operations.
    */
  def getWriter = new IndexWriter(directory, new IndexWriterConfig(new StandardAnalyzer))

  /**
    * @return a lucene [[IndexSearcher]] to be used in search operations.
    */
  def getSearcher: IndexSearcher = searcherManager.acquire()

  /**
    * refresh index content after a write operation.
    */
  def refresh(): Unit = searcherManager.maybeRefreshBlocking()

  /**
    * Validate a record before write it.
    * @param data the record to be validated.
    * @return a sequence of lucene [[Field]] that can be safely written in the index.
    */
  def validateRecord(data: T): Try[Seq[Field]]

  /**
    * Convert a lucene [[Document]] into an instance of T
    * @param document the lucene document to be converted.
    * @param fields fields that must be retrieved from the document.
    * @return the entry retrieved.
    */
  def toRecord(document: Document, fields: Seq[SimpleField]): T

  def write(fields: Set[Field])(implicit writer: IndexWriter): Try[Long] = {
    val doc = new Document
    fields.foreach(doc.add)
    Try(writer.addDocument(doc))
  }

  /**
    * write an entry into the index.
    * This method MUST NOT commit the writer.
    * @param data the entry to be written
    * @param writer a lucene [[IndexWriter]] to handle the write operation.
    * @return the lucene low level write operation return value.
    */
  protected def write(data: T)(implicit writer: IndexWriter): Try[Long]

  def delete(data: T)(implicit writer: IndexWriter): Try[Long]

  def delete(query: Query)(implicit writer: IndexWriter): Try[Long] = {
    Try {
      val result = writer.deleteDocuments(query)
      writer.forceMergeDeletes(true)
      result
    }
  }

  def deleteAll()(implicit writer: IndexWriter): Try[Long] = {
    Try {
      val result = writer.deleteAll()
      writer.forceMergeDeletes(true)
      writer.flush()
      result
    }
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

  private def executeCountQuery(searcher: IndexSearcher, query: Query, limit: Int) = {
    val hits = searcher.search(query, limit).scoreDocs.length
    val d    = new Document()
    d.add(new LongPoint(_keyField, 0))
    d.add(new IntPoint(_valueField, hits))
    Seq(d)
  }

  private[index] def rawQuery(query: Query, limit: Int, sort: Option[Sort])(
      implicit searcher: IndexSearcher): Seq[Document] = {
    executeQuery(searcher, query, limit, sort)
  }

  private[index] def rawQuery[VT, S](query: Query,
                                     collector: AllGroupsAggregationCollector[VT, S],
                                     limit: Option[Int],
                                     sort: Option[Sort]): Seq[Document] = {
    this.getSearcher.search(query, collector)

    val sortedGroupMap = sort
      .flatMap(_.getSort.headOption)
      .map(s => collector.getOrderedMap(s))
      .getOrElse(collector.getGroupMap)
      .toSeq

    val limitedGroupMap = limit.map(sortedGroupMap.take).getOrElse(sortedGroupMap)

    limitedGroupMap.map {
      case (g, v) =>
        val doc = new Document
        doc.add(collector.indexField(g, collector.groupField))
        doc.add(collector.indexField(v, collector.aggField))
        doc.add(new LongPoint(_keyField, 0))
        doc
    }
  }

  def query(query: Query, fields: Seq[SimpleField], limit: Int, sort: Option[Sort]): Seq[T] = {
    val raws = if (fields.nonEmpty && fields.forall(_.count)) {
      executeCountQuery(this.getSearcher, query, limit)
    } else
      executeQuery(this.getSearcher, query, limit, sort)
    raws.map(d => toRecord(d, fields))
  }

  def query(query: Query,
            collector: AllGroupsAggregationCollector[_, _],
            limit: Option[Int],
            sort: Option[Sort]): Seq[T] = {
    rawQuery(query, collector, limit, sort).map(d => toRecord(d, Seq.empty))
  }

  def query(field: String,
            queryString: String,
            fields: Seq[SimpleField],
            limit: Int,
            sort: Option[Sort] = None): Seq[T] = {
    val parser = new QueryParser(field, new StandardAnalyzer())
    val query  = parser.parse(queryString)

    val raws = if (fields.nonEmpty && fields.forall(_.count)) {
      executeCountQuery(this.getSearcher, query, limit)
    } else
      executeQuery(this.getSearcher, query, limit, sort)
    raws.map(d => toRecord(d, fields))
  }

  def all: Seq[T] = {
    Try { query(new MatchAllDocsQuery(), Seq.empty, Int.MaxValue, None) } match {
      case Success(docs: Seq[T]) => docs
      case Failure(_)            => Seq.empty
    }
  }
}
