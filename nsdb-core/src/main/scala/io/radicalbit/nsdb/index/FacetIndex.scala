package io.radicalbit.nsdb.index

import cats.data.Validated._
import io.radicalbit.nsdb.common.protocol.Bit
import io.radicalbit.nsdb.index.lucene.OrderedTaxonomyFacetCounts
import io.radicalbit.nsdb.validation.Validation.{FieldValidation, WriteValidation}
import org.apache.lucene.analysis.standard.StandardAnalyzer
import org.apache.lucene.document._
import org.apache.lucene.facet.taxonomy.FastTaxonomyFacetCounts
import org.apache.lucene.facet.taxonomy.directory.{DirectoryTaxonomyReader, DirectoryTaxonomyWriter}
import org.apache.lucene.facet.{FacetField, FacetResult, FacetsCollector, FacetsConfig}
import org.apache.lucene.index.{IndexWriter, IndexWriterConfig}
import org.apache.lucene.search._
import org.apache.lucene.store.BaseDirectory

import scala.util.{Failure, Success, Try}

class FacetIndex(val facetDirectory: BaseDirectory, val taxoDirectory: BaseDirectory) extends TypeSupport {

  private lazy val searcherManager: SearcherManager = new SearcherManager(facetDirectory, null)

  def getWriter = new IndexWriter(facetDirectory, new IndexWriterConfig(new StandardAnalyzer))

  def getTaxoWriter = new DirectoryTaxonomyWriter(taxoDirectory)

  def getReader = new DirectoryTaxonomyReader(taxoDirectory)

  def getSearcher: IndexSearcher = searcherManager.acquire()

  def refresh(): Unit = searcherManager.maybeRefreshBlocking()

  def release(searcher: IndexSearcher): Unit = searcherManager.release(searcher)

  def validateRecord(bit: Bit): FieldValidation =
    validateSchemaTypeSupport(bit)
      .map(se => se.flatMap(elem => elem.indexType.facetField(elem.name, elem.value)))

  def write(bit: Bit)(implicit writer: IndexWriter, taxonomyWriter: DirectoryTaxonomyWriter): WriteValidation = {
    val doc       = new Document
    val c         = new FacetsConfig
    val allFields = validateRecord(bit)

    allFields match {
      case Valid(fields) =>
        fields.foreach(f => {
          doc.add(f)
          if (f.isInstanceOf[StringField]) {
            c.setIndexFieldName(f.name, s"facet_${f.name}")
            doc.add(new FacetField(f.name, f.stringValue()))
          }
        })
        Try(writer.addDocument(c.build(taxonomyWriter, doc))) match {
          case Success(id) => valid(id)
          case Failure(ex) => invalidNel(ex.getMessage)
        }
      case errs @ Invalid(_) => errs
    }
  }

  def delete(query: Query)(implicit writer: IndexWriter): Unit = {
    writer.deleteDocuments(query)
    writer.forceMergeDeletes(true)
  }

  def delete(data: Bit)(implicit writer: IndexWriter): Unit = {
    val query = LongPoint.newExactQuery("timestamp", data.timestamp)
    writer.deleteDocuments(query)
    writer.forceMergeDeletes(true)
  }

  def deleteAll()(implicit writer: IndexWriter): Unit = {
    writer.deleteAll()
    writer.forceMergeDeletes(true)
    writer.flush()
  }

  private def getFacetResult(query: Query, groupField: String, sort: Option[Sort], limit: Option[Int]) = {
    val c = new FacetsConfig
    c.setIndexFieldName(groupField, s"facet_$groupField")

    val actualLimit = limit getOrElse Int.MaxValue

    val fc = new FacetsCollector
    sort.fold { FacetsCollector.search(getSearcher, query, actualLimit, fc) } {
      FacetsCollector.search(getSearcher, query, actualLimit, _, fc)
    }

    val facetsFolder = sort.fold(new FastTaxonomyFacetCounts(s"facet_$groupField", getReader, c, fc))(s =>
      new OrderedTaxonomyFacetCounts(s"facet_$groupField", getReader, c, fc, s))
    Option(facetsFolder.getTopChildren(actualLimit, groupField))
  }

  def getCount(query: Query, groupField: String, sort: Option[Sort], limit: Option[Int]): Seq[Bit] = {
    val facetResult: Option[FacetResult] = getFacetResult(query, groupField, sort, limit)
    facetResult.fold(Seq.empty[Bit])(
      _.labelValues.map(lv => Bit(0, lv.value.longValue(), Map(groupField -> lv.label))).toSeq)
  }

  def getDistinctField(query: Query, field: String, sort: Option[Sort], limit: Int): Seq[Bit] = {
    val facetResult = getFacetResult(query, field, sort, Some(limit))
    facetResult.fold(Seq.empty[Bit])(_.labelValues.map(lv => Bit(0, 0, Map(field -> lv.label))).toSeq)
  }
}
