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

package io.radicalbit.nsdb.actors

import java.nio.file.Paths
import java.util.concurrent.TimeUnit

import akka.actor.Actor
import io.radicalbit.nsdb.index._
import io.radicalbit.nsdb.model.Location
import org.apache.commons.io.FileUtils

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

  protected def shardsFromLocations(locations: Seq[Location]): Seq[(Location, Option[TimeSeriesIndex])] =
    locations.map(location => (location, getIndex(location)))
  protected def facetsShardsFromLocations(locations: Seq[Location]): Seq[(Location, Option[AllFacetIndexes])] =
    locations.map(location => (location, getfacetIndexesFor(location)))

  /**
    * last access for a given [[Location]]
    */
  private[actors] val shardsAccess: mutable.Map[Location, Long] = mutable.Map.empty

  /**
    * Retentions in milliseconds disseminated.
    */
  private[actors] val metricsRetention: mutable.Map[String, Long] = mutable.Map.empty

  lazy val passivateAfter: FiniteDuration = FiniteDuration(
    context.system.settings.config.getDuration("nsdb.sharding.passivate-after").toNanos,
    TimeUnit.NANOSECONDS)

  /**
    * Retrieves if exists an index for the given [[Location]]
    * @param location the location containing the metric and the time interval to identify the index to retrieve or create
    * @return the index for the given location
    */
  protected def getIndex(location: Location): Option[TimeSeriesIndex] =
    shards.get(location).map { i =>
      shardsAccess += (location -> System.currentTimeMillis())
      i
    }

  /**
    * Retrieves or creates an index for the given [[Location]]
    * @param location the location containing the metric and the time interval to identify the index to retrieve or create
    * @return the index for the given location
    */
  protected def getOrCreateIndex(location: Location): TimeSeriesIndex = {
    shardsAccess += (location -> System.currentTimeMillis())
    shards.getOrElse(
      location, {
        val directory =
          getDirectory(Paths.get(basePath, db, namespace, "shards", s"${location.shardName}"))
        val newIndex = new TimeSeriesIndex(directory)
        shards += (location -> newIndex)
        newIndex
      }
    )
  }

  /**
    * Retrieves if exists a facet index for the given [[Location]]
    * @param location the key containing the metric and the time interval to identify the index to retrieve or create
    * @return the facet index for the given location
    */
  protected def getfacetIndexesFor(location: Location): Option[AllFacetIndexes] =
    facetIndexShards.get(location).map { i =>
      shardsAccess += (location -> System.currentTimeMillis())
      i
    }

  /**
    * Retrieves or creates a facet index for the given [[Location]]
    * @param location the key containing the metric and the time interval to identify the index to retrieve or create
    * @return the facet index for the given location
    */
  protected def getOrCreatefacetIndexesFor(location: Location): AllFacetIndexes = {
    shardsAccess += (location -> System.currentTimeMillis())
    facetIndexShards.getOrElse(
      location, {
        val facetIndexes = new AllFacetIndexes(basePath = basePath,
                                               db = db,
                                               namespace = namespace,
                                               location = location,
                                               indexStorageStrategy = indexStorageStrategy)
        facetIndexShards += (location -> facetIndexes)
        facetIndexes
      }
    )
  }

  /**
    * Releases all the resources associated to a Location.
    * @param location the location to be released.
    */
  protected def releaseLocation(location: Location): Unit = {
    shards.get(location).foreach { timeSeriesIndex =>
      timeSeriesIndex.close()
    }
    shards -= location
    facetIndexShards.get(location).foreach { facetIndex =>
      facetIndex.close()
    }
    facetIndexShards -= location
    shardsAccess -= location
  }

  /**
    * delete a location from File System.
    * @param location
    */
  protected def deleteLocation(location: Location): Unit = {
    val path = Paths.get(basePath, db, namespace, "shards", s"${location.shardName}")
    if (path.toFile.exists())
      FileUtils.deleteDirectory(path.toFile)
  }

  /**
    * Convenience method to clean up indexes not being used for a configured amount of time
    */
  protected def garbageCollectIndexes(): Unit = {
    shardsAccess.foreach {
      case (location, lastAccess)
          if FiniteDuration(System.currentTimeMillis() - lastAccess, TimeUnit.MILLISECONDS) > passivateAfter =>
        releaseLocation(location)
        metricsRetention.get(location.metric).foreach {
          case retention if location.isBeyond(retention) => deleteLocation(location)
          case _                                         => //do nothing
        }
        metricsRetention -= location.metric
      case _ => // do nothing
    }
  }
}
