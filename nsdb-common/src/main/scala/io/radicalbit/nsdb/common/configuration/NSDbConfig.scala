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

package io.radicalbit.nsdb.common.configuration

object NSDbConfig {

  object HighLevel {
    final val NSDbNodeHostName          = "nsdb.node.hostname"
    final val NSDbNodePort              = "nsdb.node.port"
    final val NSDbClusterContactPointNr = "nsdb.cluster.required-contact-point-nr"
    final val NSDbClusterEndpoints      = "nsdb.cluster.endpoints"
    final val NSDBMetadataPath          = "nsdb.storage.metadata-path"

    final val CommitLogSerializer = "nsdb.commit-log.serializer"
    final val CommitLogWriter     = "nsdb.commit-log.writer"
    final val CommitLogDirectory  = "nsdb.storage.commit-log-path"
    final val CommitLogMaxSize    = "nsdb.commit-log.max-size"
    final val CommitLogBufferSize = "nsdb.commit-log.buffer-size"

    final val StorageIndexPath = "nsdb.storage.index-path"
    final val StorageTmpPath   = "nsdb.storage.tmp-path"
    final val StorageStrategy  = "nsdb.storage.strategy"

    final val GrpcInterface = "nsdb.grpc.interface"
    final val GrpcPort      = "nsdb.grpc.port"

    final val HttpInterface = "nsdb.http.interface"
    final val HttpPort      = "nsdb.http.port"
    final val HttpsPort     = "nsdb.http.https-port"

    final val ClusterMode = "nsdb.cluster.mode"

    final val MonitoringEnabled = "nsdb.monitoring.enabled"

    final val retryPolicyDelay    = "nsdb.retry-policy.delay"
    final val retryPolicyNRetries = "nsdb.retry-policy.n-retries"
  }

  object LowLevel {
    final val AkkaArteryHostName = "akka.remote.artery.canonical.hostname"
    final val AkkaArteryPort     = "akka.remote.artery.canonical.port"

    final val AkkaManagementContactPointNr = "akka.management.required-contact-point-nr"
    final val AkkaDiscoveryNSDbEndpoints   = "akka.discovery.config.services.NSDb.endpoints"
    final val AkkaDDPersistenceDir         = "akka.cluster.distributed-data.durable.lmdb.dir"
  }

}
