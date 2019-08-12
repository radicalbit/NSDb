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

import org.apache.lucene.facet.Facets;
import org.apache.lucene.facet.FacetsCollector;
import org.apache.lucene.facet.FacetsCollector.MatchingDocs;
import org.apache.lucene.index.IndexReaderContext;
import org.apache.lucene.index.ReaderUtil;
import org.apache.lucene.search.*;

import java.io.IOException;
import java.util.List;

/** {@link Facets} implementation that computes sum for
 *  dynamic long ranges from a provided {@link LongValuesSource}.  Use
 *  this for dimensions that change in real-time (e.g. a
 *  relative time based dimension like "Past day", "Past 2
 *  days", etc.) or that change for each request (e.g. 
 *  distance from the user's location, "&lt; 1 km", "&lt; 2 km",
 *  etc.).
 */
public class LongRangeFacetLongSum extends RangeFacetCounts {

  /** Create {@code LongRangeFacetCounts}, using {@link
   *  LongValuesSource} from the specified rangeField. */
  public LongRangeFacetLongSum(String rangeField, String valueField, FacetsCollector hits, LongRange... ranges) throws IOException {
    super(rangeField, ranges, null);
    sum(LongValuesSource.fromLongField(rangeField), LongValuesSource.fromLongField(valueField), hits.getMatchingDocs());
  }

  private void sum(LongValuesSource valueSource, LongValuesSource testValueSource, List<MatchingDocs> matchingDocs) throws IOException {

    LongRange[] ranges = (LongRange[]) this.ranges;

    LongRangeLongSummation counter = new LongRangeLongSummation(ranges);

    int missingCount = 0;
    for (MatchingDocs hits : matchingDocs) {
      LongValues fv = valueSource.getValues(hits.context, null);

      LongValues values = testValueSource.getValues(hits.context, null);
      
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
          counter.add(fv.longValue(),values.longValue());
        } else {
          missingCount++;
        }

        doc = docs.nextDoc();
      }
    }
    
    int x = counter.fillCounts(counts);

    missingCount += x;

    totCount -= missingCount;
  }
}
