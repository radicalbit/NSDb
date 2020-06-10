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

package io.radicalbit.nsdb.index

import io.radicalbit.nsdb.common.protocol.Bit
import io.radicalbit.nsdb.common.{NSDbNumericType, NSDbType}
import io.radicalbit.nsdb.index.lucene.OrderedTaxonomyFacetCounts
import org.apache.lucene.document.Field
import org.apache.lucene.facet.taxonomy.FastTaxonomyFacetCounts
import org.apache.lucene.facet.taxonomy.directory.DirectoryTaxonomyWriter
import org.apache.lucene.facet.{FacetField, FacetResult, FacetsCollector, FacetsConfig}
import org.apache.lucene.index.IndexWriter
import org.apache.lucene.search.{Query, Sort}
import org.apache.lucene.store.Directory

import scala.util.Try

/**
  * Index to store shard facets used for count operations.
  *
  * @param directory facet index base directory
  * @param taxoDirectory taxonomy base directory
  */
class FacetCountIndex(override val directory: Directory, override val taxoDirectory: Directory)
    extends FacetIndex(directory, taxoDirectory) {

  override protected[this] val facetNamePrefix = "facet_count_"

  override def write(bit: Bit)(implicit writer: IndexWriter, taxonomyWriter: DirectoryTaxonomyWriter): Try[Long] = {

    def facetWrite(f: Field, path: String, c: FacetsConfig): Field = {
      c.setIndexFieldName(f.name, facetName(f.name))
      new FacetField(f.name, path)
    }

    commonWrite(bit, _ => new FacetsConfig, facetWrite)
  }

  override protected[index] def internalResult(query: Query,
                                               groupField: String,
                                               sort: Option[Sort],
                                               limit: Option[Int],
                                               valueIndexType: IndexType[_] = BIGINT()): Option[FacetResult] = {
    val c = new FacetsConfig
    c.setIndexFieldName(groupField, facetName(groupField))

    val actualLimit = Int.MaxValue

    val fc = new FacetsCollector
    sort.fold { FacetsCollector.search(getSearcher, query, actualLimit, fc) } {
      FacetsCollector.search(getSearcher, query, actualLimit, _, fc)
    }

    val facetsFolder = sort.fold(new FastTaxonomyFacetCounts(facetName(groupField), getTaxoReader, c, fc))(s =>
      new OrderedTaxonomyFacetCounts(facetName(groupField), getTaxoReader, c, fc, s))

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
  override protected[index] def result(query: Query,
                                       groupField: String,
                                       sort: Option[Sort],
                                       limit: Option[Int],
                                       indexType: IndexType[_],
                                       valueIndexType: IndexType[_]): Seq[Bit] = {
    val facetResult: Option[FacetResult] = internalResult(query, groupField, sort, limit, valueIndexType)
    facetResult.fold(Seq.empty[Bit])(
      _.labelValues
        .map(
          lv =>
            Bit(
              timestamp = 0,
              value = NSDbNumericType(lv.value.longValue()),
              dimensions = Map.empty,
              tags = Map(groupField -> NSDbType(indexType.cast(lv.label)))
          ))
        .toSeq)
  }

  /**
    * Gets results from a distinct query. The distinct query can be run only using a single tag.
    * @param query query to be executed against the facet index.
    * @param field distinct field.
    * @param sort optional lucene [[Sort]]
    * @param limit results limit.
    * @return query results.
    */
  protected[index] def getDistinctField(query: Query, field: String, sort: Option[Sort], limit: Int): Seq[Bit] = {
    val res = internalResult(query, field, sort, Some(limit))
    res.fold(Seq.empty[Bit])(_.labelValues
      .map(lv =>
        Bit(timestamp = 0, value = NSDbNumericType(0), dimensions = Map.empty, tags = Map(field -> NSDbType(lv.label))))
      .toSeq)
  }
}
