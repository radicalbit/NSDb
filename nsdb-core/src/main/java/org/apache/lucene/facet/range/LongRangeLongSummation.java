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

import java.util.*;

/** Performs the summation of the quantity for each range was seen;
 *  per-hit it's just a binary search ({@link #add})
 *  against the elementary intervals, and in the end we
 *  rollup back to the original ranges. */

final class LongRangeLongSummation extends LongRangeSummation{

  private final long[] leafCounts;

  // Used during rollup
  private int leafUpto;
  private int missingCount;

  public LongRangeLongSummation(LongRange[] ranges) {
    super(ranges);
    leafCounts = new long[boundaries.length];

  }

  public void add(long v, long q) {
    // Binary search to find matched elementary range; we
    // are guaranteed to find a match because the last
    // boundary is Long.MAX_VALUE:

    int lo = 0;
    int hi = boundaries.length - 1;
    while (true) {
      int mid = (lo + hi) >>> 1;
      if (v <= boundaries[mid]) {
        if (mid == 0) {
          leafCounts[0]+=q;
          return;
        } else {
          hi = mid - 1;
        }
      } else if (v > boundaries[mid+1]) {
        lo = mid + 1;
      } else {
        leafCounts[mid+1]+=q;
        return;
      }
    }
  }

  /** Fills counts corresponding to the original input
   *  ranges, returning the missing count (how many hits
   *  didn't match any ranges). */
  public int fillCounts(int[] counts) {
    //System.out.println("  rollup");
    missingCount = 0;
    leafUpto = 0;
    rollup(root, counts, false);
    return missingCount;
  }

  private long rollup(LongRangeNode node, int[] counts, boolean sawOutputs) {
    long sum;
    sawOutputs |= node.outputs != null;
    if (node.left != null) {
      sum = rollup(node.left, counts, sawOutputs);
      sum += rollup(node.right, counts, sawOutputs);
    } else {
      // Leaf:
      sum = leafCounts[leafUpto];
      leafUpto++;
      if (!sawOutputs) {
        // This is a missing count (no output ranges were
        // seen "above" us):
        missingCount += sum;
      }
    }
    if (node.outputs != null) {
      for(int rangeIndex : node.outputs) {
        counts[rangeIndex] += sum;
      }
    }
    return sum;
  }

}
