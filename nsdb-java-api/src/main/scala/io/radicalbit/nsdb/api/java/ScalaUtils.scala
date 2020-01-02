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

package io.radicalbit.nsdb.api.java

import io.radicalbit.nsdb.common.{JDouble, JLong}
import io.radicalbit.nsdb.rpc.common.{Dimension, Tag}
import io.radicalbit.nsdb.rpc.request.RPCInsert
import io.radicalbit.nsdb.rpc.request.RPCInsert.Value

import scala.collection.JavaConverters._

/**
  * Utility class written in scala in order to provide convenience methods for a better scala - java interoperability.
  * e.g. avoiding explicit calls to scala auxiliary objects <pre>RPCInsert.Value$.Empty$.MODULE$</pre>
  */
protected object ScalaUtils {

  def emptyValue: Value.Empty.type = RPCInsert.Value.Empty

  def longValue(v: JLong) = RPCInsert.Value.LongValue(v)

  def decimalValue(v: JDouble) = RPCInsert.Value.DecimalValue(v)

  def longDimension(v: JLong) = Dimension(Dimension.Value.LongValue(v))

  def decimalDimension(v: JDouble) = Dimension(Dimension.Value.DecimalValue(v))

  def stringDimension(v: String) = Dimension(Dimension.Value.StringValue(v))

  def longTag(v: JLong) = Tag(Tag.Value.LongValue(v))

  def decimalTag(v: JDouble) = Tag(Tag.Value.DecimalValue(v))

  def stringTag(v: String) = Tag(Tag.Value.StringValue(v))

  def convertMap[K, V](jMap: java.util.Map[K, V]): Map[K, V] = jMap.asScala.toMap

}
