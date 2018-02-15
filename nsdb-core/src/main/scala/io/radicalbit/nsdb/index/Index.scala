package io.radicalbit.nsdb.index

import io.radicalbit.nsdb.index.lucene.AllGroupsAggregationCollector
import io.radicalbit.nsdb.statement.StatementParser.SimpleField
import org.apache.lucene.analysis.standard.StandardAnalyzer
import org.apache.lucene.document.Field.Store
import org.apache.lucene.document._
import org.apache.lucene.index.{DirectoryReader, IndexWriter, IndexWriterConfig}
import org.apache.lucene.queryparser.classic.QueryParser
import org.apache.lucene.search._
import org.apache.lucene.store.BaseDirectory

import scala.collection.mutable.ListBuffer
import scala.util.{Failure, Success, Try}

trait Index[T] {
  def directory: BaseDirectory

  def _keyField: String
  val _countField: String = "_count"
  val _valueField         = "value"

  private lazy val searcherManager: SearcherManager = new SearcherManager(directory, null)

  def getWriter = new IndexWriter(directory, new IndexWriterConfig(new StandardAnalyzer))

  def getSearcher: IndexSearcher = searcherManager.acquire()

  def refresh(): Unit = searcherManager.maybeRefreshBlocking()

  def release(searcher: IndexSearcher): Unit = {
    searcherManager.maybeRefreshBlocking()
    searcherManager.release(searcher)
  }

  def validateRecord(data: T): Try[Seq[Field]]
  def toRecord(document: Document, fields: Seq[SimpleField]): T

  def write(fields: Seq[Field])(implicit writer: IndexWriter): Try[Long] = {
    val doc = new Document
    fields.foreach(doc.add)
    Try(writer.addDocument(doc))
  }

  protected def write(data: T)(implicit writer: IndexWriter): Try[Long]

  def delete(data: T)(implicit writer: IndexWriter): Unit

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
    writer.forceMergeDeletes(true)
    writer.flush()
  }

  private def executeQuery(searcher: IndexSearcher, query: Query, limit: Int, sort: Option[Sort]) = {
    val docs: ListBuffer[Document] = ListBuffer.empty
    val hits =
      sort.fold(searcher.search(query, limit).scoreDocs)(sort => searcher.search(query, limit, sort).scoreDocs)
    (0 until hits.length).foreach { i =>
      val doc = searcher.doc(hits(i).doc)
      doc.add(new IntPoint(_countField, hits.length))
      docs += doc
    }
    docs.toList
  }

  private def executeCountQuery(searcher: IndexSearcher, query: Query, limit: Int) = {
    val hits = searcher.search(query, limit).scoreDocs.length
    val d    = new Document()
    d.add(new LongPoint(_keyField, 0))
    d.add(new IntPoint(_valueField, hits))
    d.add(new IntPoint(_countField, hits))
    Seq(d)
  }

  private[index] def rawQuery(query: Query, limit: Int, sort: Option[Sort])(
      implicit searcher: IndexSearcher): Seq[Document] = {
    executeQuery(searcher, query, limit, sort)
  }

  def Ord[T: Ordering](reverse: Boolean): Ordering[T] =
    if (reverse) implicitly[Ordering[T]].reverse else implicitly[Ordering[T]]

  private[index] def rawQuery(query: Query,
                              collector: AllGroupsAggregationCollector,
                              limit: Option[Int],
                              sort: Option[Sort]): Seq[Document] = {
    this.getSearcher.search(query, collector)

    val sortedGroupMap = sort
      .flatMap(_.getSort.headOption)
      .map {
        case s if s.getType == SortField.Type.STRING =>
          collector.getGroupMap.toSeq.sortBy(_._1)(Ord(s.getReverse)).toMap
        case s => collector.getGroupMap.toSeq.sortBy(_._2)(Ord(s.getReverse))
      }
      .getOrElse(collector.getGroupMap)

    val limitedGroupMap = limit.map(sortedGroupMap.take).getOrElse(sortedGroupMap)

    limitedGroupMap.map {
      case (g, v) =>
        val doc = new Document
        doc.add(new StringField(collector.groupField, g, Store.NO))
        doc.add(new LongPoint(collector.aggField, v))
        doc.add(new LongPoint(_keyField, 0))
        doc
    }.toSeq
  }

  def query(query: Query, fields: Seq[SimpleField], limit: Int, sort: Option[Sort]): Seq[T] = {
    val raws = if (fields.nonEmpty && fields.forall(_.count)) {
      executeCountQuery(this.getSearcher, query, limit)
    } else
      executeQuery(this.getSearcher, query, limit, sort)
    raws.map(d => toRecord(d, fields))
  }

  def query(query: Query, collector: AllGroupsAggregationCollector, limit: Option[Int], sort: Option[Sort]): Seq[T] = {
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

  def getAll()(implicit searcher: IndexSearcher): Seq[T] = {
    Try { query(new MatchAllDocsQuery(), Seq.empty, Int.MaxValue, None) } match {
      case Success(docs: Seq[T]) => docs
      case Failure(_)            => Seq.empty
    }
  }
}
