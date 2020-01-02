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

import java.util.ArrayList;
import java.util.List;

/** Holds one node of the segment tree. */
public final class LongRangeNode {
    final LongRangeNode left;
    final LongRangeNode right;

    // Our range, inclusive:
    final long start;
    final long end;

    // If we are a leaf, the index into elementary ranges that
    // we point to:
    final int leafIndex;

    // Which range indices to output when a query goes
    // through this node:
    List<Integer> outputs;

    LongRangeNode(long start, long end, LongRangeNode left, LongRangeNode right, int leafIndex) {
        this.start = start;
        this.end = end;
        this.left = left;
        this.right = right;
        this.leafIndex = leafIndex;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        toString(sb, 0);
        return sb.toString();
    }

    static void indent(StringBuilder sb, int depth) {
        for(int i=0;i<depth;i++) {
            sb.append("  ");
        }
    }

    /** Recursively assigns range outputs to each node. */
    void addOutputs(int index, LongRange range) {
        if (start >= range.min && end <= range.max) {
            // Our range is fully included in the incoming
            // range; add to our output list:
            if (outputs == null) {
                outputs = new ArrayList<>();
            }
            outputs.add(index);
        } else if (left != null) {
            assert right != null;
            // Recurse:
            left.addOutputs(index, range);
            right.addOutputs(index, range);
        }
    }

    void toString(StringBuilder sb, int depth) {
        indent(sb, depth);
        if (left == null) {
            assert right == null;
            sb.append("leaf: ").append(start).append(" to ").append(end);
        } else {
            sb.append("node: ").append(start).append(" to ").append(end);
        }
        if (outputs != null) {
            sb.append(" outputs=");
            sb.append(outputs);
        }
        sb.append('\n');

        if (left != null) {
            assert right != null;
            left.toString(sb, depth+1);
            right.toString(sb, depth+1);
        }
    }
}
