/*
 * Copyright 2018 Radicalbit S.r.l.
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

package io.radicalbit.nsdb.index

import io.radicalbit.nsdb.common.JSerializable
import io.radicalbit.nsdb.common.protocol.Bit
import io.radicalbit.nsdb.index.lucene.OrderedTaxonomyFacetCounts
import org.apache.lucene.document.Field
import org.apache.lucene.facet.taxonomy.FastTaxonomyFacetCounts
import org.apache.lucene.facet.taxonomy.directory.DirectoryTaxonomyWriter
import org.apache.lucene.facet.{FacetField, FacetResult, FacetsCollector, FacetsConfig}
import org.apache.lucene.index.IndexWriter
import org.apache.lucene.search.{Query, Sort}
import org.apache.lucene.store.BaseDirectory

import scala.util.Try

/**
  * Index to store shard facets used for count operations.
  *
  * @param directory facet index base directory
  * @param taxoDirectory taxonomy base directory
  */
class FacetCountIndex(override val directory: BaseDirectory, override val taxoDirectory: BaseDirectory)
    extends FacetIndex(directory, taxoDirectory) {

  override protected[this] val facetNamePrefix = "facet_count_"

  override def write(bit: Bit)(implicit writer: IndexWriter, taxonomyWriter: DirectoryTaxonomyWriter): Try[Long] = {

    def facetWrite(f: Field, path: String, c: FacetsConfig): Field = {
      c.setIndexFieldName(f.name, facetName(f.name))
      new FacetField(f.name, path)
    }

    Try(commonWrite(bit, _ => new FacetsConfig, facetWrite)).flatten
  }

  override protected[this] def internalResult(query: Query,
                                              groupField: String,
                                              sort: Option[Sort],
                                              limit: Option[Int],
                                              valueIndexType: Option[IndexType[_]] = None): Option[FacetResult] = {
    val c = new FacetsConfig
    c.setIndexFieldName(groupField, facetName(groupField))

    val actualLimit = Int.MaxValue

    val fc = new FacetsCollector
    sort.fold { FacetsCollector.search(getSearcher, query, actualLimit, fc) } {
      FacetsCollector.search(getSearcher, query, actualLimit, _, fc)
    }

    val facetsFolder = sort.fold(new FastTaxonomyFacetCounts(facetName(groupField), taxoReader, c, fc))(s =>
      new OrderedTaxonomyFacetCounts(facetName(groupField), taxoReader, c, fc, s))

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
  override def result(query: Query,
                      groupField: String,
                      sort: Option[Sort],
                      limit: Option[Int],
                      indexType: IndexType[_],
                      valueIndexType: Option[IndexType[_]] = None): Seq[Bit] = {
    val facetResult: Option[FacetResult] = internalResult(query, groupField, sort, limit, valueIndexType)
    facetResult.fold(Seq.empty[Bit])(
      _.labelValues
        .map(lv =>
          Bit(
            timestamp = 0,
            value = lv.value.longValue(),
            dimensions = Map.empty[String, JSerializable],
            tags = Map(groupField -> indexType.cast(lv.label).asInstanceOf[JSerializable])
        ))
        .toSeq)
  }
}
