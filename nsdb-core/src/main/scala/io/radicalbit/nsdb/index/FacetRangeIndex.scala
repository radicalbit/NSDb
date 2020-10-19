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
import io.radicalbit.nsdb.common.statement.{
  Aggregation,
  CountAggregation,
  MaxAggregation,
  MinAggregation,
  SumAggregation
}
import io.radicalbit.nsdb.index.FacetRangeIndex.FacetRangeResult
import io.radicalbit.nsdb.model.TimeRange
import org.apache.lucene.facet.range._
import org.apache.lucene.facet.{Facets, FacetsCollector}
import org.apache.lucene.search.{IndexSearcher, Query}

/**
  * Index to retrieve temporal range queries.
  */
class FacetRangeIndex {

  private final val lowerBoundField = "lowerBound"
  private final val upperBoundField = "upperBound"

  protected[index] def executeRangeFacet(searcher: IndexSearcher,
                                         query: Query,
                                         aggregation: Aggregation,
                                         rangeFieldName: String,
                                         valueFieldName: String,
                                         valueFieldType: Option[IndexType[_]],
                                         ranges: Seq[TimeRange]): Seq[Bit] = {
    val luceneRanges = ranges.map(r =>
      new LongRange(s"${r.lowerBound}-${r.upperBound}", r.lowerBound, r.lowerInclusive, r.upperBound, r.upperInclusive))
    val fc = new FacetsCollector
    FacetsCollector.search(searcher, query, 0, fc)
    val facets: Facets = (aggregation, valueFieldType) match {
      case (_: CountAggregation, _) =>
        new LongRangeFacetCounts(rangeFieldName, fc, luceneRanges: _*)
      case (_: SumAggregation, Some(_: DECIMAL)) =>
        new LongRangeFacetDoubleSum(rangeFieldName, valueFieldName, fc, luceneRanges: _*)
      case (_: SumAggregation, _) =>
        new LongRangeFacetLongSum(rangeFieldName, valueFieldName, fc, luceneRanges: _*)
      case (_: MaxAggregation, Some(_: DECIMAL)) =>
        new LongRangeFacetDoubleMinMax(rangeFieldName, valueFieldName, false, fc, luceneRanges: _*)
      case (_: MaxAggregation, _) =>
        new LongRangeFacetLongMinMax(rangeFieldName, valueFieldName, false, fc, luceneRanges: _*)
      case (_: MinAggregation, Some(_: DECIMAL)) =>
        new LongRangeFacetDoubleMinMax(rangeFieldName, valueFieldName, true, fc, luceneRanges: _*)
      case (_: MinAggregation, _) =>
        new LongRangeFacetLongMinMax(rangeFieldName, valueFieldName, true, fc, luceneRanges: _*)
    }
    toRecord(valueFieldType) {
      facets.getTopChildren(0, rangeFieldName).labelValues.toSeq.collect {
        case lv if lv.value.doubleValue > 0.0 =>
          val structuredLabel = lv.label.split("-").map(_.toLong)
          FacetRangeResult(structuredLabel(0), structuredLabel(1), lv.value)
      }
    }
  }

  private def toRecord(valueFieldType: Option[IndexType[_]]): Seq[FacetRangeResult] => Seq[Bit] = { facetResultSeq =>
    facetResultSeq
      .map { facetResult =>
        Bit(
          facetResult.lowerBound,
          valueFieldType.fold(NSDbNumericType(facetResult.value.longValue())) { typez =>
            if (typez.isInstanceOf[DECIMAL])
              NSDbNumericType(facetResult.value.doubleValue())
            else NSDbNumericType(facetResult.value.longValue())
          },
          Map[String, NSDbType](
            (lowerBoundField, NSDbType(facetResult.lowerBound)),
            (upperBoundField, NSDbType(facetResult.upperBound))
          ),
          Map.empty
        )
      }
  }

}

object FacetRangeIndex {
  case class FacetRangeResult(lowerBound: Long, upperBound: Long, value: Number)
}
