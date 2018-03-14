package io.radicalbit.nsdb.cluster.index

import io.radicalbit.nsdb.index.Index
import io.radicalbit.nsdb.statement.StatementParser.SimpleField
import org.apache.lucene.document.Field.Store
import org.apache.lucene.document._
import org.apache.lucene.index.{DirectoryReader, IndexWriter, Term}
import org.apache.lucene.search._
import org.apache.lucene.store.BaseDirectory

import scala.collection.JavaConverters._
import scala.util.{Failure, Success, Try}

case class Location(metric: String, node: String, from: Long, to: Long)

class MetadataIndex(override val directory: BaseDirectory) extends Index[Location] {
  override val _keyField: String = "_metric"

  override def validateRecord(data: Location): Try[Seq[Field]] = {
    Success(
      Seq(
        new StringField(_keyField, data.metric, Store.YES),
        new StringField("node", data.node, Store.YES),
        new LongPoint("from", data.from),
        new LongPoint("to", data.to),
        new NumericDocValuesField("from", data.from),
        new NumericDocValuesField("to", data.to),
        new StoredField("from", data.from),
        new StoredField("to", data.to)
      )
    )
  }

  override def write(data: Location)(implicit writer: IndexWriter): Try[Long] = {
    val doc = new Document
    validateRecord(data) match {
      case Success(fields) =>
        Try {
          fields.foreach(doc.add)
          writer.addDocument(doc)
        }
      case Failure(t) => Failure(t)
    }
  }

  override def toRecord(document: Document, fields: Seq[SimpleField]): Location = {
    val fields = document.getFields.asScala.map(f => f.name() -> f).toMap
    Location(
      document.get(_keyField),
      document.get("node"),
      fields("from").numericValue().longValue(),
      fields("to").numericValue().longValue()
    )
  }

  def getMetadata(metric: String): Seq[Location] = {
    Try(query(_keyField, metric, Seq.empty, Integer.MAX_VALUE)) match {
      case Success(metadataSeq) => metadataSeq
      case Failure(_)           => Seq.empty
    }
  }

  def getMetadata(metric: String, t: Long): Option[Location] = {
    val builder = new BooleanQuery.Builder()
    builder.add(LongPoint.newRangeQuery("to", t, Long.MaxValue), BooleanClause.Occur.SHOULD)
    builder.add(LongPoint.newRangeQuery("from", 0, t), BooleanClause.Occur.SHOULD).build()

    val reader                           = DirectoryReader.open(directory)
    implicit val searcher: IndexSearcher = new IndexSearcher(reader)

    Try(query(builder.build(), Seq.empty, Integer.MAX_VALUE, None).headOption) match {
      case Success(metadataSeq) => metadataSeq
      case Failure(_)           => None
    }
  }

  override def delete(data: Location)(implicit writer: IndexWriter): Unit = {
    val builder = new BooleanQuery.Builder()
    builder.add(new TermQuery(new Term(_keyField, data.metric)), BooleanClause.Occur.MUST)
    builder.add(new TermQuery(new Term("node", data.node)), BooleanClause.Occur.MUST)
    builder.add(LongPoint.newExactQuery("from", data.from), BooleanClause.Occur.MUST)
    builder.add(LongPoint.newExactQuery("to", data.to), BooleanClause.Occur.MUST)

    val query = builder.build()

    writer.deleteDocuments(query)
    writer.forceMergeDeletes(true)

  }

}
