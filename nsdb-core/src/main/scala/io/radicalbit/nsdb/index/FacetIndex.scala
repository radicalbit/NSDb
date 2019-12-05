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
import io.radicalbit.nsdb.common.protocol.{Bit, DimensionFieldType}
import org.apache.lucene.document._
import org.apache.lucene.facet._
import org.apache.lucene.facet.taxonomy.SearcherTaxonomyManager
import org.apache.lucene.facet.taxonomy.directory.{DirectoryTaxonomyReader, DirectoryTaxonomyWriter}
import org.apache.lucene.index.IndexWriter
import org.apache.lucene.search._
import org.apache.lucene.store.Directory

import scala.collection.immutable
import scala.util.{Failure, Success, Try}

abstract class FacetIndex(val directory: Directory, val taxoDirectory: Directory) extends AbstractStructuredIndex {

  private lazy val searchTaxonomyManager: SearcherTaxonomyManager =
    new SearcherTaxonomyManager(directory, taxoDirectory, null)

  def write(bit: Bit)(implicit writer: IndexWriter, taxonomyWriter: DirectoryTaxonomyWriter): Try[Long]

  protected[this] def facetNamePrefix: String

  protected[this] def facetName(name: String): String = s"$facetNamePrefix$name"

  protected[this] def internalResult(query: Query,
                                     groupField: String,
                                     sort: Option[Sort],
                                     limit: Option[Int],
                                     valueIndexType: Option[IndexType[_]] = None): Option[FacetResult]

  /**
    * @return a lucene [[IndexSearcher]] to be used in search operations.
    */
  override def getSearcher: IndexSearcher = searchTaxonomyManager.acquire().searcher

  /**
    * @return a taxonomy reader in sync with the index reader.
    */
  def getTaxoReader: DirectoryTaxonomyReader = searchTaxonomyManager.acquire().taxonomyReader

  /**
    * Refresh index and taxonomy after a write operation.
    */
  override def refresh(): Unit = searchTaxonomyManager.maybeRefresh()

  /**
    * Gets results from a count query.
    *
    * @param query query to be executed against the facet index.
    * @param groupField field in the group by clause.
    * @param sort optional lucene [[Sort]]
    * @param limit results limit.
    * @param groupFieldIndexType the group field [[IndexType]].
    * @return query results.
    */
  def result(query: Query,
             groupField: String,
             sort: Option[Sort],
             limit: Option[Int],
             groupFieldIndexType: IndexType[_],
             valueIndexType: Option[IndexType[_]]): Seq[Bit]

  override def validateRecord(bit: Bit): Try[immutable.Iterable[Field]] =
    validateSchemaTypeSupport(bit)
      .map(
        se =>
          se.collect {
              case (_, t) if t.fieldClassType != DimensionFieldType => t
            }
            .flatMap(elem => elem.indexType.facetField(elem.name, elem.value)))

  protected[this] def commonWrite(bit: Bit,
                                  facetConfig: Seq[Field] => FacetsConfig,
                                  facetField: (Field, String, FacetsConfig) => Field)(
      implicit writer: IndexWriter,
      taxonomyWriter: DirectoryTaxonomyWriter): Try[Long] = {

    val allFields = validateRecord(bit)

    allFields match {
      case Success(fields) =>
        val doc = new Document
        val c   = facetConfig(fields.toSeq)

        fields
          .filterNot(f => f.name() == "value")
          .foreach { f =>
            doc.add(f)
            if (f.isInstanceOf[StringField] || f.isInstanceOf[FloatPoint] || f.isInstanceOf[DoublePoint] ||
                f.isInstanceOf[IntPoint] || f.isInstanceOf[LongPoint]) {

              val path = if (f.numericValue != null) f.numericValue.toString else f.stringValue

              doc.add(facetField(f, path, c))
            }
          }

        Try(writer.addDocument(c.build(taxonomyWriter, doc))).recoverWith {
          case t =>
            t.printStackTrace()
            Failure(t)
        }
      case Failure(t) =>
        t.printStackTrace()
        Failure(t)
    }
  }

  /**
    * Gets results from a distinct query. The distinct query can be run only using a single tag.
    * @param query query to be executed against the facet index.
    * @param field distinct field.
    * @param sort optional lucene [[Sort]]
    * @param limit results limit.
    * @return query results.
    */
  def getDistinctField(query: Query, field: String, sort: Option[Sort], limit: Int): Seq[Bit] = {
    val res = internalResult(query, field, sort, Some(limit))
    res.fold(Seq.empty[Bit])(
      _.labelValues
        .map(lv =>
          Bit(timestamp = 0, value = 0, dimensions = Map.empty[String, JSerializable], tags = Map(field -> lv.label)))
        .toSeq)
  }

}
