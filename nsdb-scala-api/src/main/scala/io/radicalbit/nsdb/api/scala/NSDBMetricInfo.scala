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

package io.radicalbit.nsdb.api.scala

import io.radicalbit.nsdb.rpc.init.{InitMetricRequest, InitMetricResponse}

import scala.concurrent.Future

/**
  * Auxiliary case class useful to define a init metric request.
  * @param db the db.
  * @param namespace the namespace.
  * @param metric the metric.
  * @param shardInterval the shard interval to be used to the init metric request.
  * @param retention the retention to be used to the init metric request.
  */
case class MetricInfo protected (db: String,
                                 namespace: String,
                                 metric: String,
                                 shardInterval: Option[String],
                                 retention: Option[String]) {

  /**
    * Adds a Long timestamp to the bit.
    * @param v the timestamp.
    * @return a new instance with `v` as a timestamp.
    */
  def shardInterval(v: String): MetricInfo = copy(shardInterval = Some(v))

  /**
    * Adds a Long timestamp to the bit.
    * @param v the timestamp.
    * @return a new instance with `v` as a timestamp.
    */
  def retention(v: String): MetricInfo = copy(retention = Some(v))

}

class NSDBMetricInfo(nsdb: NSDB) {

  /**
    * Init a bit by providing all the auxiliary information inside the [[MetricInfo]]
    * @param metricInfo the [[MetricInfo]] to be written.
    * @return a Future of the result of the operation. See [[InitMetricResponse]]
    */
  def init(metricInfo: MetricInfo): Future[InitMetricResponse] =
    nsdb.client.initMetric(
      InitMetricRequest(metricInfo.db,
                        metricInfo.namespace,
                        metricInfo.metric,
                        metricInfo.shardInterval.getOrElse(""),
                        metricInfo.retention.getOrElse("")))

}
