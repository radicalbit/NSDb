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
import org.apache.lucene.document.Field
import org.apache.lucene.facet.taxonomy._
import org.apache.lucene.facet.taxonomy.directory.DirectoryTaxonomyWriter
import org.apache.lucene.facet._
import org.apache.lucene.index.IndexWriter
import org.apache.lucene.search.{Query, Sort}
import org.apache.lucene.store.BaseDirectory

import scala.util.{Failure, Try}

/**
  * Index to store shard facets used for count operations.
  *
  * @param directory facet index base directory
  * @param taxoDirectory taxonomy base directory
  */
class FacetSumIndex(override val directory: BaseDirectory, override val taxoDirectory: BaseDirectory)
    extends FacetIndex(directory, taxoDirectory) {

  override protected[this] val facetNamePrefix = "facet_sum_"

  override def write(bit: Bit)(implicit writer: IndexWriter, taxonomyWriter: DirectoryTaxonomyWriter): Try[Long] = {

    def facetWrite(f: Field, path: String, c: FacetsConfig): Field = bit.value match {
      case x: java.lang.Integer => new IntAssociationFacetField(x, f.name, path)
      case x: java.lang.Long    => new LongAssociationFacetField(x, f.name, path)
      case x: java.lang.Float   => new FloatAssociationFacetField(x, f.name, path)
      case x: java.lang.Double  => new DoubleAssociationFacetField(x, f.name, path)
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

  override protected[this] def internalResult(query: Query,
                                              groupField: String,
                                              sort: Option[Sort],
                                              limit: Option[Int],
                                              valueIndexType: Option[IndexType[_]] = None): Option[FacetResult] =
    Try {
      val config = new FacetsConfig
      config.setMultiValued(groupField, true)
      config.setIndexFieldName(groupField, facetName(groupField))

      val fc = new FacetsCollector

      val actualLimit = Int.MaxValue

      FacetsCollector.search(getSearcher, query, actualLimit, fc)

      val facetsFolder = valueIndexType match {
        case Some(_: INT)    => new TaxonomyFacetSumIntAssociations(facetName(groupField), getTaxoReader, config, fc)
        case Some(_: BIGINT) => new TaxonomyFacetSumLongAssociations(facetName(groupField), getTaxoReader, config, fc)
        case Some(_: DECIMAL) =>
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

  override def result(query: Query,
                      groupField: String,
                      sort: Option[Sort],
                      limit: Option[Int],
                      groupFieldIndexType: IndexType[_],
                      valueIndexType: Option[IndexType[_]]): Seq[Bit] = {
    val facetResult: Option[FacetResult] = internalResult(query, groupField, sort, limit, valueIndexType)
    facetResult.fold(Seq.empty[Bit])(
      _.labelValues
        .map(lv =>
          Bit(
            timestamp = 0,
            value = lv.value match {
              case x: java.lang.Integer => x.intValue()
              case x: java.lang.Long    => x.longValue()
              case x: java.lang.Float   => x.floatValue()
              case x: java.lang.Double  => x.doubleValue()
            },
            dimensions = Map.empty[String, JSerializable],
            tags = Map(groupField -> groupFieldIndexType.cast(lv.label).asInstanceOf[JSerializable])
        ))
        .toSeq)
  }
}
