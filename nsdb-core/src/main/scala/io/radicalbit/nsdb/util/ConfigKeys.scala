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

package io.radicalbit.nsdb.util

object ConfigKeys {

  val CommitLogSerializer = "nsdb.commit-log.serializer"
  val CommitLogWriter     = "nsdb.commit-log.writer"
  val CommitLogDirectory  = "nsdb.storage.commit-log-path"
  val CommitLogMaxSize    = "nsdb.commit-log.max-size"
  val CommitLogBufferSize = "nsdb.commit-log.buffer-size"

  val StorageIndexPath = "nsdb.storage.index-path"

  val GrpcInterface = "nsdb.grpc.interface"
  val GrpcPort      = "nsdb.grpc.port"

  val HttpInterface = "nsdb.http.interface"
  val HttpPort      = "nsdb.http.port"
  val HttpsPort     = "nsdb.http.https-port"

  val ClusterMode = "nsdb.cluster.mode"

  val MonitoringEnabled = "nsdb.monitoring.enabled"
}
