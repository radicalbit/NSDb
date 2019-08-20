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

package org.apache.lucene.facet.range;

import org.apache.lucene.facet.FacetResult;
import org.apache.lucene.facet.Facets;
import org.apache.lucene.facet.FacetsCollector;
import org.apache.lucene.facet.FacetsCollector.MatchingDocs;
import org.apache.lucene.facet.LabelAndValue;
import org.apache.lucene.index.IndexReaderContext;
import org.apache.lucene.index.ReaderUtil;
import org.apache.lucene.search.*;

import java.io.IOException;
import java.util.List;

/** {@link Facets} implementation that computes sum for
 *  dynamic double ranges from a provided {@link DoubleValuesSource}.  Use
 *  this for dimensions that change in real-time (e.g. a
 *  relative time based dimension like "Past day", "Past 2
 *  days", etc.) or that change for each request (e.g. 
 *  distance from the user's location, "&lt; 1 km", "&lt; 2 km",
 *  etc.).
 */
public class LongRangeFacetDoubleSum extends RangeFacetCounts {

  /** Counts, initialized in by subclass. */
  private final double[] summations;

  /** Create {@code LongRangeFacetCounts}, using {@link
   *  LongValuesSource} from the specified rangeField. */
  public LongRangeFacetDoubleSum(String rangeField, String valueField, FacetsCollector hits, LongRange... ranges) throws IOException {
    super(rangeField, ranges, null);
    summations = new double[ranges.length];
    sum(LongValuesSource.fromLongField(rangeField), DoubleValuesSource.fromDoubleField(valueField), hits.getMatchingDocs());
  }

  private void sum(LongValuesSource rangeSource, DoubleValuesSource valueSource, List<MatchingDocs> matchingDocs) throws IOException {

    LongRange[] ranges = (LongRange[]) this.ranges;

    LongRangeDoubleSummation counter = new LongRangeDoubleSummation(ranges);

    int missingCount = 0;
    for (MatchingDocs hits : matchingDocs) {
      LongValues fv = rangeSource.getValues(hits.context, null);

      DoubleValues values = valueSource.getValues(hits.context, null);
      
      totCount += hits.totalHits;
      final DocIdSetIterator fastMatchDocs;
      if (fastMatchQuery != null) {
        final IndexReaderContext topLevelContext = ReaderUtil.getTopLevelContext(hits.context);
        final IndexSearcher searcher = new IndexSearcher(topLevelContext);
        searcher.setQueryCache(null);
        final Weight fastMatchWeight = searcher.createWeight(searcher.rewrite(fastMatchQuery), ScoreMode.COMPLETE_NO_SCORES, 1);
        Scorer s = fastMatchWeight.scorer(hits.context);
        if (s == null) {
          continue;
        }
        fastMatchDocs = s.iterator();
      } else {
        fastMatchDocs = null;
      }

      DocIdSetIterator docs = hits.bits.iterator();      
      for (int doc = docs.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; ) {
        if (fastMatchDocs != null) {
          int fastMatchDoc = fastMatchDocs.docID();
          if (fastMatchDoc < doc) {
            fastMatchDoc = fastMatchDocs.advance(doc);
          }

          if (doc != fastMatchDoc) {
            doc = docs.advance(fastMatchDoc);
            continue;
          }
        }
        // Skip missing docs:
        if (fv.advanceExact(doc) && values.advanceExact(doc)) {
          counter.add(fv.longValue(),values.doubleValue());
        } else {
          missingCount++;
        }

        doc = docs.nextDoc();
      }
    }
    
    int x = counter.fillSummations(summations);

    missingCount += x;

    totCount -= missingCount;
  }

  @Override
  public FacetResult getTopChildren(int topN, String dim, String... path) {
    if (!dim.equals(field)) {
      throw new IllegalArgumentException("invalid dim \"" + dim + "\"; should be \"" + field + "\"");
    }
    if (path.length != 0) {
      throw new IllegalArgumentException("path.length should be 0");
    }
    LabelAndValue[] labelValues = new LabelAndValue[counts.length];
    for(int i=0;i<counts.length;i++) {
      labelValues[i] = new LabelAndValue(ranges[i].label, summations[i]);
    }
    return new FacetResult(dim, path, totCount, labelValues, labelValues.length);
  }
}
