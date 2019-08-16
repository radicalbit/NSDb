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

import io.radicalbit.nsdb.common.protocol.Bit
import io.radicalbit.nsdb.index.FacetRangeIndex.FacetRangeResult
import io.radicalbit.nsdb.model.TimeRange
import io.radicalbit.nsdb.statement._
import org.apache.lucene.facet.range._
import org.apache.lucene.facet.{Facets, FacetsCollector}
import org.apache.lucene.search.{IndexSearcher, Query}

/**
  * Index to retrieve temporal range queries.
  */
class FacetRangeIndex {

  def executeRangeFacet(searcher: IndexSearcher,
                        query: Query,
                        aggregationType: InternalTemporalAggregation,
                        rangeFieldName: String,
                        valueFieldName: String,
                        valueFieldType: Option[IndexType[_]],
                        ranges: Seq[TimeRange])(postProcFun: Seq[FacetRangeResult] => Seq[Bit]): Seq[Bit] = {
    val luceneRanges = ranges.map(r =>
      new LongRange(s"${r.lowerBound}-${r.upperBound}", r.lowerBound, r.lowerInclusive, r.upperBound, r.upperInclusive))
    val fc = new FacetsCollector
    FacetsCollector.search(searcher, query, 0, fc)
    val facets: Facets = (aggregationType, valueFieldType) match {
      case (InternalCountTemporalAggregation, _) => new LongRangeFacetCounts(rangeFieldName, fc, luceneRanges: _*)
      case (InternalSumTemporalAggregation, Some(_: DECIMAL)) =>
        new LongRangeFacetDoubleSum(rangeFieldName, valueFieldName, fc, luceneRanges: _*)
      case (InternalSumTemporalAggregation, _) =>
        new LongRangeFacetLongSum(rangeFieldName, valueFieldName, fc, luceneRanges: _*)
      case (InternalMaxTemporalAggregation, Some(_: DECIMAL)) =>
        new LongRangeFacetDoubleMinMax(rangeFieldName, valueFieldName, false, fc, luceneRanges: _*)
      case (InternalMaxTemporalAggregation, _) =>
        new LongRangeFacetLongMinMax(rangeFieldName, valueFieldName, false, fc, luceneRanges: _*)
      case (InternalMinTemporalAggregation, Some(_: DECIMAL)) =>
        new LongRangeFacetDoubleMinMax(rangeFieldName, valueFieldName, true, fc, luceneRanges: _*)
      case (InternalMinTemporalAggregation, _) =>
        new LongRangeFacetLongMinMax(rangeFieldName, valueFieldName, true, fc, luceneRanges: _*)
    }
    postProcFun {
      facets.getTopChildren(0, rangeFieldName).labelValues.toSeq.map { lv =>
        val structuredLabel = lv.label.split("-").map(_.toLong)
        FacetRangeResult(structuredLabel(0), structuredLabel(1), lv.value)
      }
    }
  }

}

object FacetRangeIndex {
  case class FacetRangeResult(lowerBound: Long, upperBound: Long, value: Number)
}
