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

package org.apache.lucene.facet.taxonomy;

import org.apache.lucene.document.DoublePoint;
import org.apache.lucene.facet.FacetsCollector;
import org.apache.lucene.facet.FacetsConfig;
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.util.BytesRef;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

public class TaxonomyFacetSumDoubleAssociations extends DoubleTaxonomyFacets{

    /** Create {@code TaxonomyFacetSumLongAssociations} against
     *  the default index field. */
    public TaxonomyFacetSumDoubleAssociations(TaxonomyReader taxoReader, FacetsConfig config, FacetsCollector fc) throws IOException {
        this(FacetsConfig.DEFAULT_INDEX_FIELD_NAME, taxoReader, config, fc);
    }

    /** Create {@code TaxonomyFacetSumLongAssociations} against
     *  the specified index field. */
    public TaxonomyFacetSumDoubleAssociations(String indexFieldName, TaxonomyReader taxoReader, FacetsConfig config, FacetsCollector fc) throws IOException {
        super(indexFieldName, taxoReader, config);
        sumValues(fc.getMatchingDocs());
    }

    private final void sumValues(List<FacetsCollector.MatchingDocs> matchingDocs) throws IOException {
        //System.out.println("count matchingDocs=" + matchingDocs + " facetsField=" + facetsFieldName);
        for(FacetsCollector.MatchingDocs hits : matchingDocs) {
            BinaryDocValues dv = hits.context.reader().getBinaryDocValues(indexFieldName);
            if (dv == null) { // this reader does not have DocValues for the requested category list
                continue;
            }

            DocIdSetIterator docs = hits.bits.iterator();

            int doc;
            while ((doc = docs.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
                //System.out.println("  doc=" + doc);
                // TODO: use OrdinalsReader?  we'd need to add a
                // BytesRef getAssociation()?
                if (dv.docID() < doc) {
                    dv.advance(doc);
                }
                if (dv.docID() == doc) {
                    final BytesRef bytesRef = dv.binaryValue();
                    byte[] bytes = bytesRef.bytes;
                    int end = bytesRef.offset + bytesRef.length;
                    int offset = bytesRef.offset;
                    while (offset < end) {
                        int ord = ((bytes[offset] & 0xFF) << 24) |
                                ((bytes[offset + 1] & 0xFF) << 16) |
                                ((bytes[offset + 2] & 0xFF) << 8) |
                                (bytes[offset + 3] & 0xFF);
                        offset += 4;
                        byte[] doubleBytes = Arrays.copyOfRange(bytes, offset, bytes.length);

                        offset += 8;
                        values[ord] += DoublePoint.decodeDimension(doubleBytes, 0);
                    }
                }
            }
        }
    }
}
