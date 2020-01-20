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

package io.radicalbit.nsdb.actors

import com.fasterxml.jackson.annotation.{JsonSubTypes, JsonTypeInfo}
import io.radicalbit.nsdb.common.protocol.{Bit, NSDbSerializable}
import io.radicalbit.nsdb.common.statement.DeleteSQLStatement
import io.radicalbit.nsdb.model.{Location, Schema}

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
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
@JsonSubTypes(
  Array(
    new JsonSubTypes.Type(value = classOf[DeleteShardRecordOperation], name = "DeleteShardRecordOperation"),
    new JsonSubTypes.Type(value = classOf[DeleteShardQueryOperation], name = "DeleteShardQueryOperation"),
    new JsonSubTypes.Type(value = classOf[WriteShardOperation], name = "WriteShardOperation")
  )
)
sealed trait ShardOperation extends NSDbSerializable {

  /**
    * operation namespace.
    */
  val namespace: String

  /**
    * operation key. @see ShardKey
    */
  val location: Location
}

case class DeleteShardRecordOperation(namespace: String, location: Location, bit: Bit) extends ShardOperation
case class DeleteShardQueryOperation(namespace: String,
                                     location: Location,
                                     deleteStatement: DeleteSQLStatement,
                                     schema: Schema)
    extends ShardOperation
case class WriteShardOperation(namespace: String, location: Location, bit: Bit) extends ShardOperation

object ShardOperation {

  /**
    * Gets the operation that needs to be applied if there is any error performing the input one.
    * @param action The input action.
    * @return
    */
  def getCompensation(action: ShardOperation): Option[ShardOperation] = {
    action match {
      case DeleteShardRecordOperation(namespace, location, bit) =>
        Some(WriteShardOperation(namespace, location, bit))
      case DeleteShardQueryOperation(_, _, _, _) =>
        None
      case WriteShardOperation(namespace, location, bit) =>
        Some(DeleteShardRecordOperation(namespace, location, bit))
    }
  }
}
