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

import io.radicalbit.nsdb.common.{NSDbDoubleType, NSDbIntType, NSDbLongType, NSDbNumericType, NSDbType}
import io.radicalbit.nsdb.common.protocol.Bit
import org.apache.lucene.document.Field
import org.apache.lucene.facet._
import org.apache.lucene.facet.taxonomy._
import org.apache.lucene.facet.taxonomy.directory.DirectoryTaxonomyWriter
import org.apache.lucene.index.IndexWriter
import org.apache.lucene.search.{Query, Sort}
import org.apache.lucene.store.Directory

import scala.util.{Failure, Try}

/**
  * Index to store shard facets used for count operations.
  *
  * @param directory facet index base directory
  * @param taxoDirectory taxonomy base directory
  */
class FacetSumIndex(override val directory: Directory, override val taxoDirectory: Directory)
    extends FacetIndex(directory, taxoDirectory) {

  override protected[this] val facetNamePrefix = "facet_sum_"

  override def write(bit: Bit)(implicit writer: IndexWriter, taxonomyWriter: DirectoryTaxonomyWriter): Try[Long] = {

    def facetWrite(f: Field, path: String, c: FacetsConfig): Field = bit.value match {
      case NSDbIntType(rawValue)    => new IntAssociationFacetField(rawValue, f.name, path)
      case NSDbLongType(rawValue)   => new LongAssociationFacetField(rawValue, f.name, path)
      case NSDbDoubleType(rawValue) => new DoubleAssociationFacetField(rawValue, f.name, path)
    }

    def setupConfig(fields: Seq[Field]): FacetsConfig = {
      val config = new FacetsConfig

      // setup config
      fields.map(_.name).distinct.foreach { name =>
        config.setMultiValued(name, true)
        config.setIndexFieldName(name, facetName(name))
      }
      config
    }

    commonWrite(bit, setupConfig, facetWrite)
  }

  override protected[index] def internalResult(query: Query,
                                               groupField: String,
                                               sort: Option[Sort],
                                               valueIndexType: IndexType[_]): Option[FacetResult] =
    Try {
      val config = new FacetsConfig
      config.setMultiValued(groupField, true)
      config.setIndexFieldName(groupField, facetName(groupField))

      val fc = new FacetsCollector

      val actualLimit = Int.MaxValue

      FacetsCollector.search(getSearcher, query, actualLimit, fc)

      val facetsFolder = valueIndexType match {
        case _: INT    => new TaxonomyFacetSumIntAssociations(facetName(groupField), getTaxoReader, config, fc)
        case _: BIGINT => new TaxonomyFacetSumLongAssociations(facetName(groupField), getTaxoReader, config, fc)
        case _: DECIMAL =>
          new TaxonomyFacetSumDoubleAssociations(facetName(groupField), getTaxoReader, config, fc)
      }

      Option(facetsFolder.getTopChildren(actualLimit, groupField))
    }.recoverWith {
        case t =>
          t.printStackTrace()
          Failure(t)
      }
      .toOption
      .flatten

  override protected[index] def result(query: Query,
                                       groupField: String,
                                       sort: Option[Sort],
                                       limit: Option[Int],
                                       groupFieldIndexType: IndexType[_],
                                       valueIndexType: IndexType[_]): Seq[Bit] = {
    val facetResult: Option[FacetResult] = internalResult(query, groupField, sort, valueIndexType)
    facetResult.fold(Seq.empty[Bit])(
      _.labelValues
        .map(
          lv =>
            Bit(
              timestamp = 0,
              value = NSDbNumericType(lv.value),
              dimensions = Map.empty[String, NSDbType],
              tags = Map(groupField -> NSDbType(groupFieldIndexType.cast(lv.label)))
          ))
        .toSeq)
  }
}
