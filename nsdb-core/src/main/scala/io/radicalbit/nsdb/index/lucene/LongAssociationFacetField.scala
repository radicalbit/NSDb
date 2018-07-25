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

package io.radicalbit.nsdb.index.lucene

import org.apache.lucene.facet.taxonomy.AssociationFacetField
import org.apache.lucene.util.BytesRef

object LongAssociationFacetField {
  def longToBytesRef(v: Long): BytesRef = {
    val bytes = new Array[Byte](8)
    // big-endian:
    bytes(0) = (v >> 56).toByte
    bytes(1) = (v >> 48).toByte
    bytes(2) = (v >> 40).toByte
    bytes(3) = (v >> 32).toByte
    bytes(4) = (v >> 24).toByte
    bytes(5) = (v >> 16).toByte
    bytes(6) = (v >> 8).toByte
    bytes(7) = v.toByte
    new BytesRef(bytes)
  }

  def bytesRefToLong(b: BytesRef): Long =
    ((b.bytes(b.offset) & 0xFF) << 56) |
      ((b.bytes(b.offset + 1) & 0xFF) << 48) |
      ((b.bytes(b.offset + 2) & 0xFF) << 40) |
      ((b.bytes(b.offset + 3) & 0xFF) << 32) |
      ((b.bytes(b.offset + 4) & 0xFF) << 24) |
      ((b.bytes(b.offset + 5) & 0xFF) << 16) |
      ((b.bytes(b.offset + 6) & 0xFF) << 8) |
      (b.bytes(b.offset + 7) & 0xFF)

}

class LongAssociationFacetField(assoc: Long, dim: String, path: String*)
    extends AssociationFacetField(LongAssociationFacetField.longToBytesRef(assoc), dim, path: _*) {

  override def toString: String =
    "LongAssociationFacetField(dim=" + dim + " path=" + path.toString + " value=" + assoc + ")"

}
