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

package org.apache.lucene.facet.taxonomy;

import org.apache.lucene.facet.TopOrdAndLongQueue;
import org.apache.lucene.facet.FacetResult;
import org.apache.lucene.facet.FacetsConfig;
import org.apache.lucene.facet.LabelAndValue;

import java.io.IOException;
import java.util.Map;

public abstract class LongTaxonomyFacets extends TaxonomyFacets {

    /** Per-ordinal value. */
    protected final long[] values;

    /** Sole constructor. */
    protected LongTaxonomyFacets(String indexFieldName, TaxonomyReader taxoReader, FacetsConfig config) throws IOException {
        super(indexFieldName, taxoReader, config);
        values = new long[taxoReader.getSize()];
    }

    /** Rolls up any single-valued hierarchical dimensions. */
    protected void rollup() throws IOException {
        // Rollup any necessary dims:
        for(Map.Entry<String,FacetsConfig.DimConfig> ent : config.getDimConfigs().entrySet()) {
            String dim = ent.getKey();
            FacetsConfig.DimConfig ft = ent.getValue();
            if (ft.hierarchical && ft.multiValued == false) {
                int dimRootOrd = taxoReader.getOrdinal(new FacetLabel(dim));
                // It can be -1 if this field was declared in the
                // config but never indexed:
                if (dimRootOrd > 0) {
                    values[dimRootOrd] += rollup(getChildren()[dimRootOrd]);
                }
            }
        }
    }

    private long rollup(int ord) throws IOException{
        long sum = 0;
        while (ord != TaxonomyReader.INVALID_ORDINAL) {
            long childValue = values[ord] + rollup(getChildren()[ord]);
            values[ord] = childValue;
            sum += childValue;
            ord = getSiblings()[ord];
        }
        return sum;
    }

    @Override
    public Number getSpecificValue(String dim, String... path) throws IOException {
        FacetsConfig.DimConfig dimConfig = verifyDim(dim);
        if (path.length == 0) {
            if (dimConfig.hierarchical && dimConfig.multiValued == false) {
                // ok: rolled up at search time
            } else if (dimConfig.requireDimCount && dimConfig.multiValued) {
                // ok: we indexed all ords at index time
            } else {
                throw new IllegalArgumentException("cannot return dimension-level value alone; use getTopChildren instead");
            }
        }
        int ord = taxoReader.getOrdinal(new FacetLabel(dim, path));
        if (ord < 0) {
            return -1;
        }
        return values[ord];
    }

    @Override
    public FacetResult getTopChildren(int topN, String dim, String... path) throws IOException {
        if (topN <= 0) {
            throw new IllegalArgumentException("topN must be > 0 (got: " + topN + ")");
        }
        FacetsConfig.DimConfig dimConfig = verifyDim(dim);
        FacetLabel cp = new FacetLabel(dim, path);
        int dimOrd = taxoReader.getOrdinal(cp);
        if (dimOrd == -1) {
            return null;
        }

        TopOrdAndLongQueue q = new TopOrdAndLongQueue(Math.min(taxoReader.getSize(), topN));

        long bottomValue = 0;

        int ord = getChildren()[dimOrd];
        long totValue = 0;
        int childCount = 0;

        TopOrdAndLongQueue.OrdAndValue reuse = null;
        while(ord != TaxonomyReader.INVALID_ORDINAL) {
            if (values[ord] > 0) {
                totValue += values[ord];
                childCount++;
                if (values[ord] > bottomValue) {
                    if (reuse == null) {
                        reuse = new TopOrdAndLongQueue.OrdAndValue();
                    }
                    reuse.ord = ord;
                    reuse.value = values[ord];
                    reuse = q.insertWithOverflow(reuse);
                    if (q.size() == topN) {
                        bottomValue = q.top().value;
                    }
                }
            }

            ord = getSiblings()[ord];
        }

        if (totValue == 0) {
            return null;
        }

        if (dimConfig.multiValued) {
            if (dimConfig.requireDimCount) {
                totValue = values[dimOrd];
            } else {
                // Our sum'd value is not correct, in general:
                totValue = -1;
            }
        } else {
            // Our sum'd dim value is accurate, so we keep it
        }

        LabelAndValue[] labelValues = new LabelAndValue[q.size()];
        for(int i=labelValues.length-1;i>=0;i--) {
            TopOrdAndLongQueue.OrdAndValue ordAndValue = q.pop();
            FacetLabel child = taxoReader.getPath(ordAndValue.ord);
            labelValues[i] = new LabelAndValue(child.components[cp.length], ordAndValue.value);
        }

        return new FacetResult(dim, path, totValue, labelValues, childCount);
    }
}