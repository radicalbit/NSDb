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

package io.radicalbit.nsdb.serialization

import java.io.NotSerializableException

import akka.serialization.Serializer
import io.radicalbit.nsdb.common.JSerializable
import io.radicalbit.nsdb.index.IndexType

import scala.util.Failure

class NSDbRawSerializer extends Serializer {
  override def identifier: Int = 7817

  override def toBinary(o: AnyRef): Array[Byte] =
    IndexType.fromClass(o.getClass).get.serialize(o.asInstanceOf[JSerializable])

  override def includeManifest: Boolean = true

  override def fromBinary(bytes: Array[Byte], manifestOpt: Option[Class[_]]): AnyRef = {
    //Yes, the get is deliberated. We want to throw the exception encapsulated in the Try
    val indexType = manifestOpt
      .map(manifest => IndexType.fromClass(manifest))
      .getOrElse(Failure(new NotSerializableException("no class retrieved")))
      .get
    indexType.deserialize(bytes).asInstanceOf[AnyRef]
  }
}
