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
import io.radicalbit.nsdb.index.lucene.OrderedTaxonomyFacetCounts
import io.radicalbit.nsdb.model.TypedField
import org.apache.lucene.document._
import org.apache.lucene.facet.taxonomy._
import org.apache.lucene.facet.taxonomy.directory.{DirectoryTaxonomyReader, DirectoryTaxonomyWriter}
import org.apache.lucene.facet._
import org.apache.lucene.index.IndexWriter
import org.apache.lucene.search._
import org.apache.lucene.store.BaseDirectory

import scala.util.{Failure, Success, Try}

/**
  * Index to store shard facets used for count operations.
  * @param directory facet index base directory
  * @param taxoDirectory taxonomy base directory
  */
class FacetIndex(val directory: BaseDirectory, val taxoDirectory: BaseDirectory) extends AbstractStructuredIndex {

  private val FacetCountNamePrefix = "facet_count_"

  private val FacetSumNamePrefix = "facet_sum_"

  private def facetCountName(name: String): String = s"$FacetCountNamePrefix$name"

  private def facetSumName(name: String): String = s"$FacetSumNamePrefix$name"

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
      .map(
        se =>
          se.collect {
              case t: TypedField if t.fieldClassType != DimensionFieldType => t
            }
            .flatMap(elem => elem.indexType.facetField(elem.name, elem.value)))

  private def commonWrite(bit: Bit,
                          facetConfig: Seq[Field] => FacetsConfig,
                          facetField: (Field, String, FacetsConfig) => Field)(
      implicit writer: IndexWriter,
      taxonomyWriter: DirectoryTaxonomyWriter): Try[Long] = {

    val allFields = validateRecord(bit)

    allFields match {
      case Success(fields) =>
        val doc = new Document
        val c   = facetConfig(fields)

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
            t.printStackTrace
            Failure(t)
        }
      case Failure(t) =>
        t.printStackTrace
        Failure(t)
    }
  }

  def write(bit: Bit)(implicit writer: IndexWriter, taxonomyWriter: DirectoryTaxonomyWriter): Try[Long] =
    for {
      res1 <- writeCount(bit)
      _    <- writeSum(bit)
    } yield res1

  private def writeCount(bit: Bit)(implicit writer: IndexWriter, taxonomyWriter: DirectoryTaxonomyWriter): Try[Long] = {

    def facetWrite(f: Field, path: String, c: FacetsConfig): Field = {
      c.setIndexFieldName(f.name, facetCountName(f.name))
      new FacetField(f.name, path)
    }

    commonWrite(bit, _ => new FacetsConfig, facetWrite)
  }

  private def writeSum(bit: Bit)(implicit writer: IndexWriter, taxonomyWriter: DirectoryTaxonomyWriter): Try[Long] = {

    def facetWrite(f: Field, path: String, c: FacetsConfig): Field = bit.value match {
      case x: java.lang.Integer => new IntAssociationFacetField(x, f.name, path)
      case x: java.lang.Long    => new IntAssociationFacetField(x.toInt, f.name, path)
      case x: java.lang.Float   => new FloatAssociationFacetField(x, f.name, path)
      case x: java.lang.Double  => new FloatAssociationFacetField(new java.lang.Float(x), f.name, path)
    }

    def setupConfig(fields: Seq[Field]): FacetsConfig = {
      val config = new FacetsConfig

      // setup config
      fields.map(_.name).distinct.foreach { name =>
        config.setMultiValued(name, true)
        config.setIndexFieldName(name, facetSumName(name))
      }
      config
    }

    commonWrite(bit, setupConfig, facetWrite)
  }

  private def getFacetResultCount(query: Query,
                                  groupField: String,
                                  sort: Option[Sort],
                                  limit: Option[Int]): Option[FacetResult] = {
    val c = new FacetsConfig
    c.setIndexFieldName(groupField, facetCountName(groupField))

    val actualLimit = Int.MaxValue

    val fc = new FacetsCollector
    sort.fold { FacetsCollector.search(getSearcher, query, actualLimit, fc) } {
      FacetsCollector.search(getSearcher, query, actualLimit, _, fc)
    }

    val facetsFolder = sort.fold(new FastTaxonomyFacetCounts(facetCountName(groupField), getReader, c, fc))(s =>
      new OrderedTaxonomyFacetCounts(facetCountName(groupField), getReader, c, fc, s))

    Option(facetsFolder.getTopChildren(actualLimit, groupField))
  }

  private def getFacetResultSum(query: Query,
                                groupField: String,
                                sort: Option[Sort],
                                limit: Option[Int],
                                valueIndexType: IndexType[_]): Option[FacetResult] = {

    Try {
      val config = new FacetsConfig
      config.setMultiValued(groupField, true)
      config.setIndexFieldName(groupField, facetSumName(groupField))

      val fc = new FacetsCollector

      val actualLimit = Int.MaxValue

      //      sort.map(FacetsCollector.search(getSearcher, query, actualLimit, _, fc))
      //        .getOrElse(FacetsCollector.search(getSearcher, query, actualLimit, fc))

      FacetsCollector.search(getSearcher, query, actualLimit, fc)

      val facetsFolder = valueIndexType match {
        case _: INT     => new TaxonomyFacetSumIntAssociations(facetSumName(groupField), getReader, config, fc)
        case _: BIGINT  => new TaxonomyFacetSumIntAssociations(facetSumName(groupField), getReader, config, fc)
        case _: DECIMAL => new TaxonomyFacetSumFloatAssociations(facetSumName(groupField), getReader, config, fc)
      }

      //      val facetsFolder = sort.map(new OrderedTaxonomyFacetCounts(facetSumName(groupField), getReader, config, fc, _))
      //          .getOrElse(new TaxonomyFacetSumIntAssociations(s"facet_$groupField", getReader, config, fc))

//      val facetsFolder = new TaxonomyFacetSumIntAssociations(facetSumName(groupField), getReader, config, fc)

      Option(facetsFolder.getTopChildren(actualLimit, groupField))
    }.recoverWith {
        case t =>
          t.printStackTrace
          Failure(t)
      }
      .toOption
      .flatten
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
    val facetResult: Option[FacetResult] = getFacetResultCount(query, groupField, sort, limit)
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

  /**
    * Gets results from a count query.
    * @param query query to be executed against the facet index.
    * @param groupField field in the group by clause.
    * @param sort optional lucene [[Sort]]
    * @param limit results limit.
    * @param groupFieldIndexType the group field [[IndexType]].
    * @return query results.
    */
  def getSum(query: Query,
             groupField: String,
             sort: Option[Sort],
             limit: Option[Int],
             groupFieldIndexType: IndexType[_],
             valueIndexType: IndexType[_]): Seq[Bit] = {
    val facetResult: Option[FacetResult] = getFacetResultSum(query, groupField, sort, limit, valueIndexType)
    facetResult.fold(Seq.empty[Bit])(
      _.labelValues
        .map(lv =>
          Bit(
            timestamp = 0,
            value = lv.value match {
              case x: java.lang.Integer => x.intValue().toLong
              case x: java.lang.Long    => x.longValue()
              case x: java.lang.Float   => x.floatValue().toDouble
              case x: java.lang.Double  => x.doubleValue()
            },
            dimensions = Map.empty[String, JSerializable],
            tags = Map(groupField -> groupFieldIndexType.cast(lv.label).asInstanceOf[JSerializable])
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
  def getDistinctField(query: Query, field: String, sort: Option[Sort], limit: Int): Seq[Bit] = {
    val facetResult = getFacetResultCount(query, field, sort, Some(limit))
    facetResult.fold(Seq.empty[Bit])(
      _.labelValues
        .map(lv =>
          Bit(timestamp = 0, value = 0, dimensions = Map.empty[String, JSerializable], tags = Map(field -> lv.label)))
        .toSeq)
  }
}
