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

package io.radicalbit.nsdb.cluster.logic

import akka.cluster.ddata.Replicator.{WriteAll, WriteConsistency, WriteMajority}

import scala.concurrent.duration.FiniteDuration

object WriteConsistencyLogic {

  /**
    * Provides a Write Consistency policy from a config value.
    * @param configValue the config value provided in the NSDb configuration file
    * @param timeout writing timeout
    * @throws IllegalArgumentException the configuration is not among the allowed values.
    */
  @throws[IllegalArgumentException]
  def fromConfigValue(configValue: String)(timeout: FiniteDuration): WriteConsistency = {
    configValue match {
      case "all"      => WriteAll(timeout)
      case "majority" => WriteMajority(timeout)
      case wrongConfigValue =>
        throw new IllegalArgumentException(s"$wrongConfigValue is not a valid value for write-consistency")
    }
  }
}
