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

import io.radicalbit.nsdb.common.{NSDbNumericType, NSDbType}
import io.radicalbit.nsdb.common.protocol.Bit
import org.apache.lucene.facet.{FacetResult, LabelAndValue}
import org.apache.lucene.search.{Query, Sort}

/**
  * Combiner of sum and count facet result
  */
class SumAndCountFacetCombiner(facetSumIndex: FacetSumIndex, facetCountIndex: FacetCountIndex) {

  def executeSumAndCountFacet(query: Query,
                              groupField: String,
                              sort: Option[Sort],
                              limit: Option[Int],
                              indexType: IndexType[_],
                              valueIndexType: IndexType[_]): Seq[Bit] = {
    val facetSumResult   = facetSumIndex.internalResult(query, groupField, sort, limit, valueIndexType)
    val facetCountResult = facetCountIndex.internalResult(query, groupField, sort, limit, valueIndexType)

    combineSumAndCount(facetSumResult, facetCountResult).map {
      case (groupFieldValue, (sum, count)) =>
        Bit(
          timestamp = 0,
          value = NSDbNumericType(0),
          dimensions = Map.empty[String, NSDbType],
          tags = Map(
            groupField -> NSDbType(indexType.cast(groupFieldValue)),
            "sum"      -> NSDbNumericType(sum),
            "count"    -> NSDbNumericType(count.longValue())
          )
        )
    }.toSeq
  }

  private def turnToLabelAndValues(facetResult: Option[FacetResult]) =
    facetResult
      .map(_.labelValues.toList)
      .getOrElse(List.empty[LabelAndValue])
      .map(lav => lav.label -> lav.value)
      .toMap

  private def combineSumAndCountLabelValues(keys: Set[String],
                                            labelAndValuesSumAggr: Map[String, Number],
                                            labelAndValuesCountAggr: Map[String, Number]) =
    keys.map { key =>
      (labelAndValuesSumAggr.get(key), labelAndValuesCountAggr.get(key)) match {
        case (Some(sumValue), Some(countValue)) =>
          key -> (sumValue, countValue)
        case _ =>
          throw new RuntimeException("No corresponding value either for sum or for count or both")
      }
    }.toMap

  private def combineSumAndCount(facetSumResult: Option[FacetResult],
                                 facetCountResult: Option[FacetResult]): Map[String, (Number, Number)] = {
    val labelAndValuesSumAggr   = turnToLabelAndValues(facetSumResult)
    val labelAndValuesCountAggr = turnToLabelAndValues(facetCountResult)
    val keys                    = labelAndValuesSumAggr.keys.toSet ++ labelAndValuesCountAggr.keys.toSet
    combineSumAndCountLabelValues(keys, labelAndValuesSumAggr, labelAndValuesCountAggr)
  }
}
