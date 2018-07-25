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

object DoubleAssociationFacetField {
  def doubleToBytesRef(v: Double): BytesRef = {
    LongAssociationFacetField.longToBytesRef(java.lang.Double.doubleToLongBits(v))
  }

  def bytesRefToFloat(b: BytesRef): Double = {
    java.lang.Double.longBitsToDouble(LongAssociationFacetField.bytesRefToLong(b))
  }
}

class DoubleAssociationFacetField(assoc: Double, dim: String, path: String*)
    extends AssociationFacetField(DoubleAssociationFacetField.doubleToBytesRef(assoc), dim, path: _*) {

  override def toString: String =
    "DoubleAssociationFacetField(dim=" + dim + " path=" + path.toString + " value=" + assoc + ")"
}
