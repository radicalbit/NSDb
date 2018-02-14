package io.radicalbit.nsdb.actors

import io.radicalbit.nsdb.common.protocol.Bit
import org.apache.lucene.search.Query

sealed trait Operation {
  val ns: String
  val metric: String
}

case class DeleteRecordOperation(ns: String, metric: String, bit: Bit)    extends Operation
case class DeleteQueryOperation(ns: String, metric: String, query: Query) extends Operation
case class WriteOperation(ns: String, metric: String, bit: Bit)           extends Operation
