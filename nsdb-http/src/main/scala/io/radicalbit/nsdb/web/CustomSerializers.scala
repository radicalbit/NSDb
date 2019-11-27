package io.radicalbit.nsdb.web

import io.radicalbit.nsdb.common.statement.{Aggregation, CountAggregation, MaxAggregation, MinAggregation, SumAggregation}
import org.json4s
import org.json4s.{CustomSerializer, JNull, JString}

object CustomSerializers {

  case object AggregationSerializer extends CustomSerializer[Aggregation](_ => ({
        case JString(aggregation) =>
          aggregation match {
            case "count" => CountAggregation
            case "max" => MaxAggregation
            case "min" => MinAggregation
            case "sum" => SumAggregation
          }
        case JNull => null
      }, {
        case CountAggregation => JString("count")
        case MaxAggregation => JString("max")
        case MinAggregation => JString("min")
        case SumAggregation => JString("sum")
      }))


  val customSerializers = List(AggregationSerializer)

}
