package io.radicalbit.nsdb.minicluster.converters

import io.radicalbit.nsdb.api.scala.{Db, Bit => ApiBit}
import io.radicalbit.nsdb.common.JSerializable
import io.radicalbit.nsdb.common.protocol.Bit
import io.radicalbit.nsdb.rpc.common.{Dimension, Tag}
import io.radicalbit.nsdb.rpc.request.RPCInsert

/**
  * Object containing implicit class useful to convert [[Bit]] into [[ApiBit]] and vice versa.
  */
object BitConverters {

  implicit class BitConverter(bit: Bit) {

    def asApiBit(db: String, namespace: String, metric: String): ApiBit = {
      val initialBit = Db(db).namespace(namespace).metric(metric).timestamp(bit.timestamp)
      val apiBit = bit.value match {
        case v: java.lang.Long       => initialBit.value(v)
        case v: java.lang.Double     => initialBit.value(v)
        case v: java.lang.Float      => initialBit.value(v.toDouble)
        case v: java.lang.Integer    => initialBit.value(v)
        case v: java.math.BigDecimal => initialBit.value(v)
      }

      bit.dimensions.foreach {
        case (k, v: java.lang.Double)     => apiBit.dimension(k, v)
        case (k, v: java.lang.Float)      => apiBit.dimension(k, v.toDouble)
        case (k, v: java.lang.Long)       => apiBit.dimension(k, v)
        case (k, v: java.lang.Integer)    => apiBit.dimension(k, v)
        case (k, v: java.math.BigDecimal) => apiBit.dimension(k, v)
        case (k, v: String)               => apiBit.dimension(k, v)
      }

      bit.tags.foreach {
        case (k, v: java.lang.Double)     => apiBit.tag(k, v)
        case (k, v: java.lang.Float)      => apiBit.tag(k, v.toDouble)
        case (k, v: java.lang.Long)       => apiBit.tag(k, v)
        case (k, v: java.lang.Integer)    => apiBit.tag(k, v)
        case (k, v: java.math.BigDecimal) => apiBit.tag(k, v)
        case (k, v: String)               => apiBit.tag(k, v)
      }
      apiBit
    }
  }

  implicit class ApiBitConverter(bit: ApiBit) {
    private def valueFor(v: RPCInsert.Value): JSerializable = v match {
      case _: RPCInsert.Value.DecimalValue => v.decimalValue.get
      case _: RPCInsert.Value.LongValue    => v.longValue.get
    }

    private def dimensionFor(v: Dimension.Value): JSerializable = v match {
      case _: Dimension.Value.DecimalValue => v.decimalValue.get
      case _: Dimension.Value.LongValue    => v.longValue.get
      case _                               => v.stringValue.get
    }

    private def tagFor(v: Tag.Value): JSerializable = v match {
      case _: Tag.Value.DecimalValue => v.decimalValue.get
      case _: Tag.Value.LongValue    => v.longValue.get
      case _                         => v.stringValue.get
    }

    def asCommonBit =
      Bit(
        timestamp = bit.timestamp.getOrElse(System.currentTimeMillis()),
        dimensions = bit.dimensions.collect {
          case (k, v) if !v.value.isStringValue || v.getStringValue != "" => (k, dimensionFor(v.value))
        }.toMap,
        tags = bit.tags.collect {
          case (k, v) if !v.value.isStringValue || v.getStringValue != "" => (k, tagFor(v.value))
        }.toMap,
        value = valueFor(bit.value)
      )
  }

}
