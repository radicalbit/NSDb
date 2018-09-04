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

package org.apache.lucene.facet;

import org.apache.lucene.document.DoublePoint;
import org.apache.lucene.facet.taxonomy.AssociationFacetField;
import org.apache.lucene.util.BytesRef;

import java.util.Arrays;

/**
 * Used to write sum facets for Double fields
 */
public class DoubleAssociationFacetField extends AssociationFacetField {
    /**
     * Creates this from {@code dim} and {@code path} and a double
     * association
     *
     * @param assoc {@code double} association
     * @param dim dimension
     * @param path
     */
    public DoubleAssociationFacetField(double assoc, String dim, String... path) {
        super(doubleToBytesRef(assoc), dim, path);
    }

    /**
     * Encodes an {@code double} as a 8-byte {@link BytesRef},
     * big-endian.
     *
     * @param v {@code double} value
     * @return {@link BytesRef} representation of double
     */
    static BytesRef doubleToBytesRef(double v) {
        byte[] bytes = new byte[8];
        DoublePoint.encodeDimension(v, bytes, 0);
        return new BytesRef(bytes);
    }

    @Override
    public String toString() {
        return "DoubleAssociationFacetField(dim=" + dim + " path=" + Arrays.toString(path) + " value=" + assoc + ")";
    }
}
