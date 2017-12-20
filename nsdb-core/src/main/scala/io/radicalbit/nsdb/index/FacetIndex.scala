package io.radicalbit.nsdb.index

import cats.data.Validated.{Invalid, Valid, invalidNel, valid}
import io.radicalbit.nsdb.common.protocol.Bit
import io.radicalbit.nsdb.validation.Validation.{FieldValidation, WriteValidation}
import org.apache.lucene.analysis.standard.StandardAnalyzer
import org.apache.lucene.document._
import org.apache.lucene.facet.taxonomy.directory.{DirectoryTaxonomyReader, DirectoryTaxonomyWriter}
import org.apache.lucene.facet.{FacetField, FacetsConfig}
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

  def release(searcher: IndexSearcher): Unit = {
    searcherManager.maybeRefreshBlocking()
    searcherManager.release(searcher)
  }

  def validateRecord(bit: Bit): FieldValidation =
    validateSchemaTypeSupport(bit)
      .map(se => se.flatMap(elem => elem.indexType.facetField(elem.name, elem.indexType.cast(elem.value))))

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

  def getGroups(query: Query, groupField: String, limit: Int): Seq[Bit] = {
    import org.apache.lucene.facet.FacetsCollector
    import org.apache.lucene.facet.taxonomy.FastTaxonomyFacetCounts

    val c = new FacetsConfig
    c.setIndexFieldName(groupField, s"facet_$groupField")

    val fc = new FacetsCollector
    FacetsCollector.search(getSearcher, query, limit, fc)
    val facetsFolder = new FastTaxonomyFacetCounts(s"facet_$groupField", getReader, c, fc)
    val x            = facetsFolder.getTopChildren(limit, groupField)
    x.labelValues.map(lv => Bit(0, lv.value.longValue(), Map(groupField -> lv.label))).toSeq
  }
}
