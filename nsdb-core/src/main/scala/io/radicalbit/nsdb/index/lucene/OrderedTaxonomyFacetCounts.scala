package io.radicalbit.nsdb.index.lucene

import org.apache.lucene.facet._
import org.apache.lucene.facet.taxonomy.{FacetLabel, FastTaxonomyFacetCounts, TaxonomyReader}
import org.apache.lucene.search.Sort
import org.apache.lucene.search.SortField.Type

class OrderedTaxonomyFacetCounts(
    indexFieldName: String,
    taxoReader: TaxonomyReader,
    config: FacetsConfig,
    fc: FacetsCollector,
    order: Sort
) extends FastTaxonomyFacetCounts(indexFieldName: String,
                                    taxoReader: TaxonomyReader,
                                    config: FacetsConfig,
                                    fc: FacetsCollector) {
  override def getTopChildren(topN: Int, dim: String, path: String*): FacetResult = {
    if (topN <= 0) throw new IllegalArgumentException("topN must be > 0 (got: " + topN + ")")
    val dimConfig: FacetsConfig.DimConfig = verifyDim(dim)
    val cp: FacetLabel                    = new FacetLabel(dim, path.toArray)
    val dimOrd                            = taxoReader.getOrdinal(cp)
    if (dimOrd == -1) return null

    val q = new TopOrdAndIntQueue(taxoReader.getSize)

    var bottomValue = 0

    var ord        = children(dimOrd)
    var totValue   = 0
    var childCount = 0

    var reuse: TopOrdAndIntQueue.OrdAndValue = null
    while (ord != TaxonomyReader.INVALID_ORDINAL) {
      if (values(ord) > 0) {
        totValue += values(ord)
        childCount += 1
        if (values(ord) > bottomValue) {
          if (reuse == null) reuse = new TopOrdAndIntQueue.OrdAndValue
          reuse.ord = ord
          reuse.value = values(ord)
          reuse = q.insertWithOverflow(reuse)
        }
      }
      ord = siblings(ord)
    }

    if (totValue == 0)
      null
    else {
      if (dimConfig.multiValued)
        if (dimConfig.requireDimCount) totValue = values(dimOrd)
        else { // Our sum'd value is not correct, in general:
          totValue = -1
        } else {
        // Our sum'd dim value is accurate, so we keep it
      }

      val sortedField = order.getSort.head.getType
      val isReverse   = order.getSort.head.getReverse
      implicit val labelAndValueOrdering: Ordering[LabelAndValue] = {
        val ordering = sortedField match {
          case Type.STRING => Ordering.by[LabelAndValue, String](e => e.label)
          case Type.LONG   => Ordering.by[LabelAndValue, Long](e => e.value.longValue())
          case Type.INT    => Ordering.by[LabelAndValue, Int](e => e.value.intValue())
          case Type.DOUBLE => Ordering.by[LabelAndValue, Double](e => e.value.doubleValue())

        }
        if (isReverse) ordering.reverse else ordering
      }

      val labelsAndValues = toList(Array.empty[LabelAndValue], q, cp)

      new FacetResult(dim, path.toArray, totValue, labelsAndValues.sorted.take(topN), childCount)
    }

  }

  private def toList(la: Array[LabelAndValue],
                     queue: TopOrdAndIntQueue,
                     facetLabel: FacetLabel): Array[LabelAndValue] = {
    if (queue.size() != 0) {
      val ordAndValue   = queue.pop
      val child         = taxoReader.getPath(ordAndValue.ord)
      val labelAndValue = new LabelAndValue(child.components(facetLabel.length), ordAndValue.value)
      val newArray      = la :+ labelAndValue
      toList(newArray, queue, facetLabel)
    } else la
  }

}
