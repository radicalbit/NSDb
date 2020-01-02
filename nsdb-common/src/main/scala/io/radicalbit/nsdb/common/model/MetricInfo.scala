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

package io.radicalbit.nsdb.common.model

/**
  * Metric Info.
  *
  * @param db the db.
  * @param namespace the namespace.
  * @param metric        the metric.
  * @param shardInterval shard interval for the metric in milliseconds.
  * @param retention     period in which data is stored for the metric, 0 means infinite.
  */
case class MetricInfo(db: String, namespace: String, metric: String, shardInterval: Long, retention: Long = 0)
