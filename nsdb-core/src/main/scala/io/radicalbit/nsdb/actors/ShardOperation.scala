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

package io.radicalbit.nsdb.actors

import io.radicalbit.nsdb.common.protocol.Bit
import io.radicalbit.nsdb.model.Location
import org.apache.lucene.search.Query

/**
  * shard operations accumulated by [[MetricAccumulatorActor]] and performed by [[MetricPerformerActor]]
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
    * operation key. @see ShardKey
    */
  val location: Location

  /**
    * Operation that needs to be applied if there is any error performing the parent one.
    */
  val compensation: Option[ShardOperation]
}

case class DeleteShardRecordOperation(namespace: String, location: Location, bit: Bit) extends ShardOperation {
  override val compensation: Option[ShardOperation] = None
}
case class DeleteShardQueryOperation(namespace: String, location: Location, query: Query) extends ShardOperation {
  override val compensation: Option[ShardOperation] = None
}
case class WriteShardOperation(namespace: String, location: Location, bit: Bit) extends ShardOperation {
  override val compensation: Option[ShardOperation] = Some(DeleteShardRecordOperation(namespace, location, bit))
}
