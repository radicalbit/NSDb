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

package org.apache.lucene.facet.range;

import java.util.*;

/** Performs the summation of the Double quantity for each range was seen;
 *  per-hit it's just a binary search ({@link #add})
 *  against the elementary intervals, and in the end we
 *  rollup back to the original ranges. */
final class LongRangeDoubleSummation extends LongRangeSummation{

  private final double[] leafCounts;

  // Used during rollup
  private int leafUpto;
  private int missingCount;

  public LongRangeDoubleSummation(LongRange[] ranges) {
    super(ranges);
    leafCounts = new double[boundaries.length];

  }

  public void add(long v, double q) {
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

  /** Fills summations corresponding to the original input
   *  ranges, returning the missing count (how many hits
   *  didn't match any ranges). */
  public int fillSummations(double[] summations) {
    //System.out.println("  rollup");
    missingCount = 0;
    leafUpto = 0;
    rollup(root, summations, false);
    return missingCount;
  }

  private double rollup(LongRangeNode node, double[] summations, boolean sawOutputs) {
    double sum;
    sawOutputs |= node.outputs != null;
    if (node.left != null) {
      sum = rollup(node.left, summations, sawOutputs);
      sum += rollup(node.right, summations, sawOutputs);
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
        summations[rangeIndex] += sum;
      }
    }
    return sum;
  }
}
