package io.radicalbit.nsdb.actors

import io.radicalbit.nsdb.common.protocol.Bit
import org.apache.lucene.search.Query

/**
  * key to identify a shard.
  * @param metric shard's metric.
  * @param from shard's location lower bound.
  * @param to shard's location upper bound.
  */
case class ShardKey(metric: String, from: Long, to: Long)

/**
  * shard operations accumulated by [[ShardAccumulatorActor]] and performed by [[ShardPerformerActor]]
  * Subclasses are
  *
  * - [[DeleteShardRecordOperation]] delete a record from a shard.
  *
  * - [[DeleteShardQueryOperation]] delete records that fulfills a given query.
  *
  * - [[WriteShardOperation writes]] add a record to a shard.
  */
sealed trait ShardOperation {
  /**
    * operation namespace.
    */
  val namespace: String
  /**
    * operation key. {@see ShardKey}
    */
  val shardKey: ShardKey
}

case class DeleteShardRecordOperation(namespace: String, shardKey: ShardKey, bit: Bit)    extends ShardOperation
case class DeleteShardQueryOperation(namespace: String, shardKey: ShardKey, query: Query) extends ShardOperation
case class WriteShardOperation(namespace: String, shardKey: ShardKey, bit: Bit)           extends ShardOperation
