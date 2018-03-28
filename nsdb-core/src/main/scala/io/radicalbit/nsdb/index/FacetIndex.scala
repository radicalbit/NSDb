package io.radicalbit.nsdb.index

import io.radicalbit.nsdb.common.JSerializable
import io.radicalbit.nsdb.common.protocol.Bit
import io.radicalbit.nsdb.index.lucene.OrderedTaxonomyFacetCounts
import org.apache.lucene.document._
import org.apache.lucene.facet.taxonomy.FastTaxonomyFacetCounts
import org.apache.lucene.facet.taxonomy.directory.{DirectoryTaxonomyReader, DirectoryTaxonomyWriter}
import org.apache.lucene.facet.{FacetField, FacetResult, FacetsCollector, FacetsConfig}
import org.apache.lucene.index.IndexWriter
import org.apache.lucene.search._
import org.apache.lucene.store.BaseDirectory

import scala.util.{Failure, Success, Try}

/**
  * Index to store shard facets used for count operations.
  * @param directory facet index base directory
  * @param taxoDirectory taxonomy base directory
  */
class FacetIndex(val directory: BaseDirectory, val taxoDirectory: BaseDirectory) extends AbstractTimeSeriesIndex {

  /**
    * @return the [[org.apache.lucene.facet.taxonomy.TaxonomyWriter]]
    */
  def getTaxoWriter = new DirectoryTaxonomyWriter(taxoDirectory)

  /**
    * @return the [[org.apache.lucene.facet.taxonomy.TaxonomyReader]]
    */
  def getReader = new DirectoryTaxonomyReader(taxoDirectory)

  override def validateRecord(bit: Bit): Try[Seq[Field]] =
    validateSchemaTypeSupport(bit)
      .map(se => se.flatMap(elem => elem.indexType.facetField(elem.name, elem.value)))

  def write(bit: Bit)(implicit writer: IndexWriter, taxonomyWriter: DirectoryTaxonomyWriter): Try[Long] = {
    val doc       = new Document
    val c         = new FacetsConfig
    val allFields = validateRecord(bit)

    allFields match {
      case Success(fields) =>
        fields
          .filterNot(f => f.name() == "value")
          .foreach(f => {
            doc.add(f)
            if (f.isInstanceOf[StringField] || f.isInstanceOf[DoublePoint] || f.isInstanceOf[LongPoint] || f
                  .isInstanceOf[IntPoint]) {
              c.setIndexFieldName(f.name, s"facet_${f.name}")
              if (f.numericValue() != null) {
                doc.add(new FacetField(f.name, f.numericValue().toString))
              } else
                doc.add(new FacetField(f.name, f.stringValue()))
            }
          })
        Try(writer.addDocument(c.build(taxonomyWriter, doc)))
      case Failure(t) => Failure(t)
    }
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

  /**
    * Gets results from a count query.
    * @param query query to be executed against the facet index.
    * @param groupField field in the group by clause.
    * @param sort optional lucene [[Sort]]
    * @param limit results limit.
    * @param indexType the group field [[IndexType]].
    * @return query results.
    */
  def getCount(query: Query,
               groupField: String,
               sort: Option[Sort],
               limit: Option[Int],
               indexType: IndexType[_]): Seq[Bit] = {
    val facetResult: Option[FacetResult] = getFacetResult(query, groupField, sort, limit)
    facetResult.fold(Seq.empty[Bit])(
      _.labelValues
        .map(lv =>
          Bit(0, lv.value.longValue(), Map(groupField -> indexType.cast(lv.label).asInstanceOf[JSerializable])))
        .toSeq)
  }

  /**
    * Gets results from a distinct query.
    * @param query query to be executed against the facet index.
    * @param field distinct field.
    * @param sort optional lucene [[Sort]]
    * @param limit results limit.
    * @return query results.
    */
  def getDistinctField(query: Query, field: String, sort: Option[Sort], limit: Int): Seq[Bit] = {
    val facetResult = getFacetResult(query, field, sort, Some(limit))
    facetResult.fold(Seq.empty[Bit])(_.labelValues.map(lv => Bit(0, 0, Map(field -> lv.label))).toSeq)
  }
}
