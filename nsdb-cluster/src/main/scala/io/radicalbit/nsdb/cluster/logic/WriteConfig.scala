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

import java.util.concurrent.TimeUnit

import akka.actor.Actor
import akka.cluster.ddata.Replicator.{WriteAll, WriteConsistency, WriteLocal, WriteMajority}
import io.radicalbit.nsdb.common.configuration.NSDbConfig.HighLevel.globalTimeout

import scala.concurrent.duration.FiniteDuration

trait WriteConfig { this: Actor =>
  import io.radicalbit.nsdb.cluster.logic.WriteConfig._
  import MetadataConsistency._

  private val config = context.system.settings.config

  private lazy val timeout: FiniteDuration =
    FiniteDuration(config.getDuration(globalTimeout, TimeUnit.SECONDS), TimeUnit.SECONDS)

  implicit def metadataConsistency2WriteConsistency(inputString: MetadataConsistency): WriteConsistency =
    inputString match {
      case All      => WriteAll(timeout)
      case Majority => WriteMajority(timeout)
      case Local    => WriteLocal
    }

  /**
    * Provides a Write Consistency policy from a config value.
    */
  protected lazy val metadataWriteConsistency: WriteConsistency =
    MetadataConsistency(config.getString("nsdb.cluster.metadata-write-consistency"))

  /**
    * Parallel write-processing guarantees higher throughput while serial write-processing preserves the order of the operations.
    */
  sealed trait WriteProcessing
  case object Parallel extends WriteProcessing
  case object Serial   extends WriteProcessing

  implicit def string2WriteProcessing(inputString: String): WriteProcessing =
    inputString.toLowerCase match {
      case "parallel" => Parallel
      case "serial"   => Serial
      case wrongConfigValue =>
        throw new IllegalArgumentException(s"$wrongConfigValue is not a valid value for write-processing")
    }

  protected lazy val writeProcessing: WriteProcessing = config.getString("nsdb.cluster.write-processing")

}

object WriteConfig {

  object MetadataConsistency {

    /**
      * Metadata consistency
      * - All: a metadata record is synchronously disseminated to all cluster nodes.
      * - All: a metadata record is synchronously disseminated to the nodes, asynchronously to the remaining.
      * - Local: a metadata record is written only to the local node and then asynchronously disseminate to the others.
      */
    sealed trait MetadataConsistency

    case object All      extends MetadataConsistency
    case object Majority extends MetadataConsistency
    case object Local    extends MetadataConsistency

    def apply(inputString: String): MetadataConsistency =
      inputString.toLowerCase match {
        case "all"      => All
        case "majority" => Majority
        case "local"    => Local
        case wrongConfigValue =>
          throw new IllegalArgumentException(s"$wrongConfigValue is not a valid value for metadata-write-consistency")
      }
  }
}
