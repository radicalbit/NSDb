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
import io.radicalbit.nsdb.common.protocol.{Bit, Coordinates}
import io.radicalbit.nsdb.common.statement.{DeleteSQLStatement, SelectSQLStatement}
import io.radicalbit.nsdb.model.{Location, Schema}

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

    case class PutSchemaInCache(db: String, namespace: String, metric: String, schema: Schema)
    case class GetSchemaFromCache(db: String, namespace: String, metric: String)
    case class EvictSchema(db: String, namespace: String, metric: String)

    case class ExecuteStatement(selectStatement: SelectSQLStatement)
    case class ExecuteSelectStatement(selectStatement: SelectSQLStatement, schema: Schema, locations: Seq[Location])

    case class FlatInput(ts: Long, db: String, namespace: String, metric: String, data: Array[Byte])
    case class MapInput(ts: Long, db: String, namespace: String, metric: String, record: Bit)
    case class PublishRecord(db: String, namespace: String, metric: String, record: Bit, schema: Schema)
    case class ExecuteDeleteStatement(statement: DeleteSQLStatement)
    case class ExecuteDeleteStatementInShards(statement: DeleteSQLStatement, schema: Schema, keys: Seq[Location])
    case class DropMetric(db: String, namespace: String, metric: String)
    case class DeleteNamespace(db: String, namespace: String)

    case class UpdateSchemaFromRecord(db: String, namespace: String, metric: String, record: Bit)
    case class UpdateSchema(db: String, namespace: String, metric: String, newSchema: Schema)
    case class DeleteSchema(db: String, namespace: String, metric: String)
    case class DeleteAllSchemas(db: String, namespace: String)

    case class GetCount(db: String, namespace: String, metric: String)
    case class AddRecordToShard(db: String, namespace: String, location: Location, bit: Bit)
    case class DeleteRecordFromShard(db: String, namespace: String, shardKey: Location, bit: Bit)
    case class DeleteAllMetrics(db: String, namespace: String)

    case object GetNodeChildActors
    case class NodeChildActorsGot(metadataCoordinator: ActorRef,
                                  writeCoordinator: ActorRef,
                                  readCoordinator: ActorRef,
                                  schemaCoordinator: ActorRef,
                                  publisher: ActorRef)

    case class SubscribeMetricsDataActorReads(actor: ActorRef, nodeName: String)
    case class SubscribeMetricsDataActorWrites(actor: ActorRef, nodeName: String)
    case class SubscribeCommitLogCoordinator(actor: ActorRef, nodeName: String)
    case class SubscribePublisher(actor: ActorRef, nodeName: String)

    case object GetConnectedDataNodes

    case object GetMetricsDataActorsReads
    case object GetMetricsDataActorsWrites
    case object GetCommitLogCoordinators
    case object GetPublishers
  }

  /**
    * events received from nsdb actors.
    */
  object Events {

    case object WarmUpCompleted

    sealed trait ErrorCode
    case class MetricNotFound(metric: String) extends ErrorCode
    case object Generic                       extends ErrorCode

    case class DbsGot(dbs: Set[String])
    case class NamespacesGot(db: String, namespaces: Set[String])
    case class SchemaGot(db: String, namespace: String, metric: String, schema: Option[Schema])
    case class GetSchemaFailed(db: String, namespace: String, metric: String, reason: String)
    case class SchemaCached(db: String, namespace: String, metric: String, schema: Option[Schema])
    case class MetricsGot(db: String, namespace: String, metrics: Set[String])
    case class SelectStatementExecuted(db: String, namespace: String, metric: String, values: Seq[Bit])
    case class SelectStatementFailed(reason: String, errorCode: ErrorCode = Generic)

    sealed trait WriteCoordinatorResponse {
      def location: Location = Location(Coordinates("", "", ""), "", 0, 0)
      def timestamp: Long    = System.currentTimeMillis()
    }

    case class InputMapped(db: String, namespace: String, metric: String, record: Bit) extends WriteCoordinatorResponse
    case class RecordAdded(db: String,
                           namespace: String,
                           metric: String,
                           record: Bit,
                           override val location: Location,
                           override val timestamp: Long)
        extends WriteCoordinatorResponse
    case class RecordRejected(db: String,
                              namespace: String,
                              metric: String,
                              record: Bit,
                              override val location: Location,
                              reasons: List[String],
                              override val timestamp: Long)
        extends WriteCoordinatorResponse

    case class DeleteStatementExecuted(db: String, namespace: String, metric: String)
    case class DeleteStatementFailed(db: String, namespace: String, metric: String, reason: String)
    case class MetricDropped(db: String, namespace: String, metric: String)
    case class NamespaceDeleted(db: String, namespace: String)
    case class DeleteNamespaceFailed(db: String, namespace: String, reason: String)

    case class SchemaUpdated(db: String, namespace: String, metric: String, schema: Schema)
    case class UpdateSchemaFailed(db: String, namespace: String, metric: String, errors: List[String])
    case class SchemaDeleted(db: String, namespace: String, metric: String)
    case class AllSchemasDeleted(db: String, namespace: String)

    case class CountGot(db: String, namespace: String, metric: String, count: Int)
    case class RecordDeleted(db: String, namespace: String, metric: String, record: Bit)
    case class AllMetricsDeleted(db: String, namespace: String)

    case class CommitLogCoordinatorSubscribed(actor: ActorRef, nodeName: String)
    case class MetricsDataActorSubscribed(actor: ActorRef, nodeName: String)
    case class PublisherSubscribed(actor: ActorRef, nodeName: String)

    case class ConnectedDataNodesGot(nodes: Seq[String])
  }

}
