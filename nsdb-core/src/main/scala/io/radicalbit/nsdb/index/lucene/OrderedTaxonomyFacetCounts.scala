/*
 * Copyright 2018-2020 Radicalbit S.r.l.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.radicalbit.nsdb.index.lucene

import org.apache.lucene.facet._
import org.apache.lucene.facet.taxonomy.{FastTaxonomyFacetCounts, TaxonomyReader}
import org.apache.lucene.search.Sort
import org.apache.lucene.search.SortField.Type

/**
  * Implementation of FastTaxonomyFacetCounts handling results ordering and applying limit as last operation.
  *
  * @param indexFieldName: grouping dimension name
  * @param taxoReader: searcher used to access categories by ordinal
  * @param config
  * @param fc
  * @param order: Sort class used to define sorting
  */
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

    val facetResult = Option(super.getTopChildren(topN, dim, path: _*))

    val sortedField = order.getSort.head.getType
    val isReverse   = order.getSort.head.getReverse
    implicit val labelAndValueOrdering: Ordering[LabelAndValue] = {
      val ordering = sortedField match {
        case Type.STRING => Ordering.by[LabelAndValue, String](e => e.label)
        case Type.LONG   => Ordering.by[LabelAndValue, Long](e => e.value.longValue())
        case Type.INT    => Ordering.by[LabelAndValue, Int](e => e.value.intValue())
        case Type.DOUBLE => Ordering.by[LabelAndValue, Double](e => e.value.doubleValue())
        case _           => Ordering.by[LabelAndValue, String](e => e.label)
      }
      if (isReverse) ordering.reverse else ordering
    }

    facetResult.map { fr =>
      new FacetResult(fr.dim, fr.path, fr.value, fr.labelValues.sorted.take(topN), fr.childCount)
    } getOrElse new FacetResult(dim, path.toArray, 0, Array.empty, 0)
  }
}
