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

package io.radicalbit.nsdb.protocol

import akka.actor.ActorRef
import akka.dispatch.ControlMessage
import io.radicalbit.nsdb.actors.ShardKey
import io.radicalbit.nsdb.common.protocol.Bit
import io.radicalbit.nsdb.common.statement.{DeleteSQLStatement, SelectSQLStatement}
import io.radicalbit.nsdb.model.Schema

/**
  * common messages exchanged among all the nsdb actors.
  */
object MessageProtocol {

  /**
    * commands executed among nsdb actors.
    */
  object Commands {
    case object GetDbs                                   extends ControlMessage
    case class GetNamespaces(db: String)                 extends ControlMessage
    case class GetMetrics(db: String, namespace: String) extends ControlMessage
    case class GetSchema(db: String, namespace: String, metric: String)
    case class ExecuteStatement(selectStatement: SelectSQLStatement, replyTo: ActorRef = ActorRef.noSender)
    case class ExecuteSelectStatement(selectStatement: SelectSQLStatement, schema: Schema)

    case class FlatInput(ts: Long, db: String, namespace: String, metric: String, data: Array[Byte])
    case class MapInput(ts: Long, db: String, namespace: String, metric: String, record: Bit)
    case class PublishRecord(db: String, namespace: String, metric: String, record: Bit, schema: Schema)
    case class ExecuteDeleteStatement(statement: DeleteSQLStatement)
    case class ExecuteDeleteStatementInShards(statement: DeleteSQLStatement, schema: Schema, keys: Seq[ShardKey])
    case class DropMetric(db: String, namespace: String, metric: String)
    case class DeleteNamespace(db: String, namespace: String)

    case class UpdateSchemaFromRecord(db: String, namespace: String, metric: String, record: Bit)
    case class DeleteSchema(db: String, namespace: String, metric: String)
    case class DeleteAllSchemas(db: String, namespace: String)

    case class GetCount(db: String, namespace: String, metric: String)
    case class AddRecordToShard(db: String, namespace: String, shardKey: ShardKey, bit: Bit)
    case class DeleteRecordFromShard(db: String, namespace: String, shardKey: ShardKey, bit: Bit)
    case class DeleteAllMetrics(db: String, namespace: String)

    case object GetReadCoordinator
    case object GetWriteCoordinator
    case object GetPublisher

    case class SubscribeMetricsDataActor(actor: ActorRef, nodeName: String)
    case class SubscribeCommitLogActor(actor: ActorRef, nodeName: Option[String] = None)
  }

  /**
    * events received from nsdb actors.
    */
  object Events {

    sealed trait ErrorCode
    case class MetricNotFound(metric: String) extends ErrorCode
    case object Generic                       extends ErrorCode

    case class DbsGot(dbs: Set[String])
    case class NamespacesGot(db: String, namespaces: Set[String])
    case class SchemaGot(db: String, namespace: String, metric: String, schema: Option[Schema])
    case class MetricsGot(db: String, namespace: String, metrics: Set[String])
    case class SelectStatementExecuted(db: String, namespace: String, metric: String, queryString: String, quid: Option[String] = None, values: Seq[Bit])
    case class SelectStatementFailed(reason: String, errorCode: ErrorCode = Generic)

    case class InputMapped(db: String, namespace: String, metric: String, record: Bit)
    case class DeleteStatementExecuted(db: String, namespace: String, metric: String)
    case class DeleteStatementFailed(db: String, namespace: String, metric: String, reason: String)
    case class MetricDropped(db: String, namespace: String, metric: String)
    case class NamespaceDeleted(db: String, namespace: String)

    case class SchemaUpdated(db: String, namespace: String, metric: String, schema: Schema)
    case class UpdateSchemaFailed(db: String, namespace: String, metric: String, errors: List[String])
    case class SchemaDeleted(db: String, namespace: String, metric: String)
    case class AllSchemasDeleted(db: String, namespace: String)

    case class CountGot(db: String, namespace: String, metric: String, count: Int)
    case class RecordAdded(db: String, namespace: String, metric: String, record: Bit)
    case class RecordRejected(db: String, namespace: String, metric: String, record: Bit, reasons: List[String])
    case class RecordDeleted(db: String, namespace: String, metric: String, record: Bit)
    case class AllMetricsDeleted(db: String, namespace: String)

    case class CommitLogActorSubscribed(actor: ActorRef, host: Option[String] = None)
    case class CommitLogActorSubscriptionFailed(actor: ActorRef, host: Option[String] = None, reason: String)

    case class MetricsDataActorSubscribed(actor: ActorRef, nodeName: String)
    case class NamespaceDataActorSubscriptionFailed(actor: ActorRef, host: Option[String] = None, reason: String)
  }

}
