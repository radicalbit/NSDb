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

package io.radicalbit.nsdb.protocol

import akka.actor.ActorRef
import akka.dispatch.ControlMessage
import com.fasterxml.jackson.annotation.{JsonSubTypes, JsonTypeInfo}
import io.radicalbit.nsdb.common.model.MetricInfo
import io.radicalbit.nsdb.common.protocol.{Bit, NSDbSerializable}
import io.radicalbit.nsdb.common.statement.{DeleteSQLStatement, SelectSQLStatement}
import io.radicalbit.nsdb.model.{Location, Schema, TimeRange}

/**
  * common messages exchanged among all the nsdb actors.
  */
object MessageProtocol {

  /**
    * commands executed among NSDb actors.
    */
  object Commands {
    case object GetDbs                                                  extends ControlMessage with NSDbSerializable
    case class GetNamespaces(db: String)                                extends ControlMessage with NSDbSerializable
    case class GetMetrics(db: String, namespace: String)                extends ControlMessage with NSDbSerializable
    case class GetSchema(db: String, namespace: String, metric: String) extends NSDbSerializable

    case class GetMetricInfo(db: String, namespace: String, metric: String) extends NSDbSerializable

    case class PutSchemaInCache(db: String, namespace: String, metric: String, schema: Schema) extends NSDbSerializable
    case class GetSchemaFromCache(db: String, namespace: String, metric: String)               extends NSDbSerializable
    case class EvictSchema(db: String, namespace: String, metric: String)                      extends NSDbSerializable

    case class ValidateStatement(selectStatement: SelectSQLStatement) extends NSDbSerializable
    case class ExecuteStatement(selectStatement: SelectSQLStatement)  extends NSDbSerializable
    case class ExecuteSelectStatement(selectStatement: SelectSQLStatement,
                                      schema: Schema,
                                      locations: Seq[Location],
                                      ranges: Seq[TimeRange] = Seq.empty,
                                      isSingleNode: Boolean)
        extends NSDbSerializable

    case class FlatInput(ts: Long, db: String, namespace: String, metric: String, data: Array[Byte])
        extends NSDbSerializable
    case class MapInput(ts: Long, db: String, namespace: String, metric: String, record: Bit) extends NSDbSerializable
    case class PublishRecord(db: String, namespace: String, metric: String, record: Bit, schema: Schema)
        extends NSDbSerializable
    case class ExecuteDeleteStatement(statement: DeleteSQLStatement) extends NSDbSerializable
    case class ExecuteDeleteStatementInShards(statement: DeleteSQLStatement, schema: Schema, keys: Seq[Location])
        extends NSDbSerializable
    case class EvictShard(db: String, namespace: String, location: Location) extends NSDbSerializable

    case class DropMetric(db: String, namespace: String, metric: String) extends NSDbSerializable
    case class DropMetricWithLocations(db: String, namespace: String, metric: String, locations: Seq[Location])
        extends NSDbSerializable
    case class DeleteNamespace(db: String, namespace: String) extends NSDbSerializable

    case class DisseminateRetention(db: String, namespace: String, metric: String, retention: Long)
        extends NSDbSerializable

    case class UpdateSchemaFromRecord(db: String, namespace: String, metric: String, record: Bit)
        extends NSDbSerializable
    case class UpdateSchema(db: String, namespace: String, metric: String, newSchema: Schema) extends NSDbSerializable
    case class DeleteSchema(db: String, namespace: String, metric: String)                    extends NSDbSerializable
    case class DeleteAllSchemas(db: String, namespace: String)                                extends NSDbSerializable

    case class GetCountWithLocations(db: String, namespace: String, metric: String, locations: Seq[Location])
        extends NSDbSerializable
    case class AddRecordToShard(db: String, namespace: String, location: Location, bit: Bit) extends NSDbSerializable
    case class DeleteRecordFromShard(db: String, namespace: String, shardKey: Location, bit: Bit)
        extends NSDbSerializable
    case class DeleteAllMetrics(db: String, namespace: String) extends NSDbSerializable

    case object GetNodeChildActors extends NSDbSerializable
    case class NodeChildActorsGot(metadataCoordinator: ActorRef,
                                  writeCoordinator: ActorRef,
                                  readCoordinator: ActorRef,
                                  publisher: ActorRef)
        extends NSDbSerializable

    case class SubscribeMetricsDataActor(actor: ActorRef, nodeName: String)     extends NSDbSerializable
    case class UnsubscribeMetricsDataActor(nodeName: String)                    extends NSDbSerializable
    case class SubscribeCommitLogCoordinator(actor: ActorRef, nodeName: String) extends NSDbSerializable
    case class UnSubscribeCommitLogCoordinator(nodeName: String)                extends NSDbSerializable
    case class SubscribePublisher(actor: ActorRef, nodeName: String)            extends NSDbSerializable
    case class UnSubscribePublisher(nodeName: String)                           extends NSDbSerializable

    case object GetMetricsDataActors     extends NSDbSerializable
    case object GetCommitLogCoordinators extends NSDbSerializable
    case object GetPublishers            extends NSDbSerializable
  }

  /**
    * events received from nsdb actors.
    */
  object Events {

    @JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
    @JsonSubTypes(
      Array(
        new JsonSubTypes.Type(value = classOf[MetricNotFound], name = "MetricNotFound"),
        new JsonSubTypes.Type(value = classOf[Generic], name = "Generic")
      ))
    sealed trait ErrorCode
    case class MetricNotFound(metric: String) extends ErrorCode with NSDbSerializable
    case class Generic()                      extends ErrorCode with NSDbSerializable

    case class DbsGot(dbs: Set[String])                           extends NSDbSerializable
    case class NamespacesGot(db: String, namespaces: Set[String]) extends NSDbSerializable

    @JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
    @JsonSubTypes(
      Array(
        new JsonSubTypes.Type(value = classOf[GetSchemaResponse], name = "GetSchemaResponse"),
        new JsonSubTypes.Type(value = classOf[GetSchemaFailed], name = "GetSchemaFailed")
      ))
    trait GetSchemaResponse extends NSDbSerializable
    case class SchemaGot(db: String, namespace: String, metric: String, schema: Option[Schema])
        extends GetSchemaResponse
    case class GetSchemaFailed(db: String, namespace: String, metric: String, reason: String) extends GetSchemaResponse

    case class MetricInfoGot(db: String, namespace: String, metric: String, metricInfo: Option[MetricInfo])
        extends NSDbSerializable
    case class SchemaCached(db: String, namespace: String, metric: String, schema: Option[Schema])
        extends NSDbSerializable
    case class MetricsGot(db: String, namespace: String, metrics: Set[String]) extends NSDbSerializable

    case class SelectStatementValidated(statement: SelectSQLStatement) extends NSDbSerializable
    case class SelectStatementValidationFailed(statement: SelectSQLStatement,
                                               reason: String,
                                               errorCode: ErrorCode = Generic())
        extends NSDbSerializable

    @JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
    @JsonSubTypes(
      Array(
        new JsonSubTypes.Type(value = classOf[SelectStatementExecuted], name = "SelectStatementExecuted"),
        new JsonSubTypes.Type(value = classOf[SelectStatementFailed], name = "SelectStatementFailed")
      ))
    trait ExecuteSelectStatementResponse extends NSDbSerializable
    case class SelectStatementExecuted(statement: SelectSQLStatement, values: Seq[Bit])
        extends ExecuteSelectStatementResponse
    case class SelectStatementFailed(statement: SelectSQLStatement, reason: String, errorCode: ErrorCode = Generic())
        extends ExecuteSelectStatementResponse

    sealed trait WriteCoordinatorResponse {
      def location: Location = Location.empty
      def timestamp: Long    = System.currentTimeMillis()
    }

    case class InputMapped(db: String, namespace: String, metric: String, record: Bit)
        extends WriteCoordinatorResponse
        with NSDbSerializable
    case class RecordAdded(db: String,
                           namespace: String,
                           metric: String,
                           record: Bit,
                           override val location: Location,
                           override val timestamp: Long)
        extends WriteCoordinatorResponse
        with NSDbSerializable
    case class RecordRejected(db: String,
                              namespace: String,
                              metric: String,
                              record: Bit,
                              override val location: Location,
                              reasons: List[String],
                              override val timestamp: Long)
        extends WriteCoordinatorResponse
        with NSDbSerializable

    case class DeleteStatementExecuted(db: String, namespace: String, metric: String) extends NSDbSerializable
    case class DeleteStatementFailed(db: String,
                                     namespace: String,
                                     metric: String,
                                     reason: String,
                                     errorCode: ErrorCode = Generic())
        extends NSDbSerializable
    case class MetricDropped(db: String, namespace: String, metric: String)         extends NSDbSerializable
    case class NamespaceDeleted(db: String, namespace: String)                      extends NSDbSerializable
    case class DeleteNamespaceFailed(db: String, namespace: String, reason: String) extends NSDbSerializable

    @JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
    @JsonSubTypes(
      Array(
        new JsonSubTypes.Type(value = classOf[SchemaUpdated], name = "SchemaUpdated"),
        new JsonSubTypes.Type(value = classOf[UpdateSchemaFailed], name = "UpdateSchemaFailed")
      ))
    sealed trait SchemaUpdateResponse
    case class SchemaUpdated(db: String, namespace: String, metric: String, schema: Schema)
        extends SchemaUpdateResponse
        with NSDbSerializable
    case class UpdateSchemaFailed(db: String, namespace: String, metric: String, errors: List[String])
        extends SchemaUpdateResponse
        with NSDbSerializable

    case class SchemaDeleted(db: String, namespace: String, metric: String) extends NSDbSerializable
    case class AllSchemasDeleted(db: String, namespace: String)             extends NSDbSerializable

    case class CountGot(db: String, namespace: String, metric: String, count: Int)       extends NSDbSerializable
    case class RecordDeleted(db: String, namespace: String, metric: String, record: Bit) extends NSDbSerializable
    case class AllMetricsDeleted(db: String, namespace: String)                          extends NSDbSerializable
    case class ShardEvicted(db: String, namespace: String, location: Location)           extends NSDbSerializable
    case class EvictedShardFailed(db: String, namespace: String, location: Location, reason: String)
        extends NSDbSerializable

    case class CommitLogCoordinatorSubscribed(actor: ActorRef, nodeName: String) extends NSDbSerializable
    case class MetricsDataActorSubscribed(actor: ActorRef, nodeName: String)     extends NSDbSerializable
    case class PublisherSubscribed(actor: ActorRef, nodeName: String)            extends NSDbSerializable

    case class CommitLogCoordinatorUnSubscribed(nodeName: String) extends NSDbSerializable
    case class MetricsDataActorUnSubscribed(nodeName: String)     extends NSDbSerializable
    case class PublisherUnSubscribed(nodeName: String)            extends NSDbSerializable

    case class MigrationStarted(inputPath: String) extends NSDbSerializable
  }

}
