package io.radicalbit.nsdb.actors

import io.radicalbit.nsdb.common.protocol.Bit
import org.apache.lucene.search.Query

case class ShardKey(metric: String, from: Long, to: Long)

sealed trait ShardOperation {
  val ns: String
  val shardKey: ShardKey
}

case class DeleteShardRecordOperation(ns: String, shardKey: ShardKey, bit: Bit)    extends ShardOperation
case class DeleteShardQueryOperation(ns: String, shardKey: ShardKey, query: Query) extends ShardOperation
case class WriteShardOperation(ns: String, shardKey: ShardKey, bit: Bit)           extends ShardOperation
