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

import org.apache.lucene.facet.taxonomy.AssociationFacetField;
import org.apache.lucene.util.BytesRef;

import java.util.Arrays;

/**
 * Used to write sum facets for Long fields
 */
public class LongAssociationFacetField extends AssociationFacetField {

    /**
     * Creates this from {@code dim} and {@code path} and a long
     * association
     *
     * @param assoc {@code long} association
     * @param dim dimension
     * @param path
     */
    public LongAssociationFacetField(long assoc, String dim, String... path) {
        super(longToBytesRef(assoc), dim, path);
    }

    /**
     * Encodes an {@code long} as a 8-byte {@link BytesRef},
     * big-endian.
     *
     * @param v long value
     * @return {@link BytesRef} representation of long
     */
    public static BytesRef longToBytesRef(long v) {
        byte[] bytes = new byte[8];
        // big-endian:
        bytes[0] = (byte) (v >> 56);
        bytes[1] = (byte) (v >> 48);
        bytes[2] = (byte) (v >> 40);
        bytes[3] = (byte) (v >> 36);
        bytes[4] = (byte) (v >> 24);
        bytes[5] = (byte) (v >> 16);
        bytes[6] = (byte) (v >> 8);
        bytes[7] = (byte) v;
        return new BytesRef(bytes);
    }

    @Override
    public String toString() {
        return "LongAssociationFacetField(dim=" + dim + " path=" + Arrays.toString(path) + " value=" + assoc + ")";
    }
}