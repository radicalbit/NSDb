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

import akka.actor.Actor
import io.radicalbit.nsdb.index.{FacetIndex, TimeSeriesIndex}
import org.apache.lucene.store.MMapDirectory

import scala.collection.mutable

/**
  * Trait containing common operation to be executed on shards indexes.
  * Mixed in by [[ShardAccumulatorActor]] and [[ShardPerformerActor]],
  *
  */
trait ShardsActor { this: Actor =>

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
    * all index shards for the given db and namespace grouped by [[ShardKey]]
    */
  private[actors] val shards: mutable.Map[ShardKey, TimeSeriesIndex] = mutable.Map.empty

  /**
    * all facet index shards for the given db and namespace grouped by [[ShardKey]]
    */
  protected val facetIndexShards: mutable.Map[ShardKey, FacetIndex] = mutable.Map.empty

  protected def shardsForMetric(metric: String)        = shards.filter(_._1.metric == metric)
  protected def facetsShardsFromMetric(metric: String) = facetIndexShards.filter(_._1.metric == metric)

  /**
    * Retrieves or creates an index for the given [[ShardKey]]
    * @param key the key containing the metric and the time interval to identify the index to retrieve or create
    * @return the index for the key
    */
  protected def getIndex(key: ShardKey) =
    shards.getOrElse(
      key, {
        val directory =
          new MMapDirectory(Paths.get(basePath, db, namespace, "shards", s"${key.metric}_${key.from}_${key.to}"))
        val newIndex = new TimeSeriesIndex(directory)
        shards += (key -> newIndex)
        newIndex
      }
    )

  /**
    * Retrieves or creates a facet index for the given [[ShardKey]]
    * @param key the key containing the metric and the time interval to identify the index to retrieve or create
    * @return the facet index for the key
    */
  protected def getFacetIndex(key: ShardKey) =
    facetIndexShards.getOrElse(
      key, {
        val directory =
          new MMapDirectory(
            Paths.get(basePath, db, namespace, "shards", s"${key.metric}_${key.from}_${key.to}", "facet"))
        val taxoDirectory = new MMapDirectory(
          Paths.get(basePath, db, namespace, "shards", s"${key.metric}_${key.from}_${key.to}", "facet", "taxo"))
        val newIndex = new FacetIndex(directory, taxoDirectory)
        facetIndexShards += (key -> newIndex)
        newIndex
      }
    )

}
