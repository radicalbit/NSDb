package io.radicalbit.nsdb.api.java

import io.radicalbit.nsdb.common.{JDouble, JLong}
import io.radicalbit.nsdb.rpc.common.Dimension
import io.radicalbit.nsdb.rpc.request.RPCInsert

import scala.collection.JavaConverters._

protected object ScalaUtils {

  def emptyValue = RPCInsert.Value.Empty

  def longValue(v: JLong) = RPCInsert.Value.LongValue(v)

  def decimalValue(v: JDouble) = RPCInsert.Value.DecimalValue(v)

  def longDimension(v: JLong) = Dimension(Dimension.Value.LongValue(v))

  def decimalDimension(v: JDouble) = Dimension(Dimension.Value.DecimalValue(v))

  def stringDimension(v: String) = Dimension(Dimension.Value.StringValue(v))

  def convertMap[K, V](jMap: java.util.Map[K, V]) = jMap.asScala.toMap

}
