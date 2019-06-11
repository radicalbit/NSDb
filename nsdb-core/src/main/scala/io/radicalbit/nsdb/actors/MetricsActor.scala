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

package io.radicalbit.nsdb.actors

import java.nio.file.Paths
import java.util.concurrent.TimeUnit

import akka.actor.Actor
import io.radicalbit.nsdb.index._
import io.radicalbit.nsdb.model.Location

import scala.collection.mutable
import scala.concurrent.duration.FiniteDuration

/**
  * Trait containing common operation to be executed on metrics indexes.
  * Mixed in by [[MetricAccumulatorActor]] and [[MetricPerformerActor]],
  */
trait MetricsActor extends DirectorySupport { this: Actor =>

  /**
    * basePath shards indexes path.
    */
  val basePath: String

  /**
    * shards db.
    */
  val db: String

  /**
    * shards namespace.
    */
  val namespace: String

  /**
    * all index shards for the given db and namespace grouped by [[Location]]
    */
  private[actors] val shards: mutable.Map[Location, TimeSeriesIndex] = mutable.Map.empty

  /**
    * all facet shard indexes for the given db and namespace grouped by [[Location]]
    */
  private[actors] val facetIndexShards: mutable.Map[Location, AllFacetIndexes] = mutable.Map.empty

  protected def shardsFromLocations(locations: Seq[Location]): Seq[(Location, TimeSeriesIndex)] =
    locations.map(location => (location, getIndex(location)))
  protected def facetsShardsFromLocations(locations: Seq[Location]): Seq[(Location, AllFacetIndexes)] =
    locations.map(location => (location, facetIndexesFor(location)))
  /**
    * last access for a given [[Location]]
    */
  private[actors] val shardsAccess: mutable.Map[Location, Long] = mutable.Map.empty

  lazy val passivateAfter = FiniteDuration(
    context.system.settings.config.getDuration("nsdb.sharding.passivate-after").toNanos,
    TimeUnit.NANOSECONDS)

  /**
    * Retrieves or creates an index for the given [[Location]]
    * @param location the location containing the metric and the time interval to identify the index to retrieve or create
    * @return the index for the given location
    */
  protected def getIndex(location: Location): TimeSeriesIndex = {
    shardsAccess += (location -> System.currentTimeMillis())
    shards.getOrElse(
      location, {
        val directory =
          createMmapDirectory(
            Paths.get(basePath, db, namespace, "shards", s"${location.metric}_${location.from}_${location.to}"))
        val newIndex = new TimeSeriesIndex(directory)
        shards += (location -> newIndex)
        newIndex
      }
    )
  }

  /**
    * Retrieves or creates a facet index for the given [[Location]]
    * @param location the key containing the metric and the time interval to identify the index to retrieve or create
    * @return the facet index for the given location
    */
  protected def facetIndexesFor(location: Location): AllFacetIndexes = {
    shardsAccess += (location -> System.currentTimeMillis())
    facetIndexShards.getOrElse(
      location, {
        val facetIndexes = new AllFacetIndexes(basePath = basePath, db = db, namespace = namespace, location = location)
        facetIndexShards += (location -> facetIndexes)
        facetIndexes
      }
    )
  }

  /**
    * Convenience method to clean up indexes not being used for a configured amount of time
    */
  protected def garbageCollectIndexes(): Unit = {
    val now = System.currentTimeMillis()
    shardsAccess.foreach {
      case (location, lastAccess) if FiniteDuration(now - lastAccess, TimeUnit.MILLISECONDS) > passivateAfter =>
        shards.get(location).foreach { timeSeriesIndex =>
          timeSeriesIndex.close()
        }
        shards -= location
        facetIndexShards.get(location).foreach { facetIndex =>
          facetIndex.close()
        }
        facetIndexShards -= location
      case _ => // do nothing
    }
  }
}
