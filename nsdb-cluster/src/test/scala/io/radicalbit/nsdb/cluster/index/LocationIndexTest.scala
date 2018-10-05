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

package io.radicalbit.nsdb.cluster.index

import io.radicalbit.nsdb.common.protocol.Coordinates
import io.radicalbit.nsdb.model.Location
import org.apache.lucene.analysis.standard.StandardAnalyzer
import org.apache.lucene.index.{IndexWriter, IndexWriterConfig}
import org.apache.lucene.store.RAMDirectory
import org.scalatest.{FlatSpec, Matchers, OneInstancePerTest}

class LocationIndexTest extends FlatSpec with Matchers with OneInstancePerTest {

  "LocationsIndex" should "write and read properly" in {

    lazy val directory = new RAMDirectory()

    implicit val writer = new IndexWriter(directory, new IndexWriterConfig(new StandardAnalyzer))

    val metadataIndex = new LocationIndex(directory)

    (0 to 100).foreach { i =>
      val testData = Location(Coordinates("db", "namespace", s"metric_$i"), s"node_$i", 0, 0)
      metadataIndex.write(testData)
    }
    writer.close()

    (0 to 100).foreach { i =>
      val result = metadataIndex.getLocationsForCoordinates(Coordinates("db", "namespace", s"metric_$i"))
      result.size shouldBe 1
    }

    val firstMetadata = metadataIndex.getLocationsForCoordinates(Coordinates("db", "namespace", "metric_0"))

    firstMetadata shouldBe List(
      Location(Coordinates("db", "namespace", s"metric_0"), s"node_0", 0, 0)
    )
  }

  "LocationsIndex" should "get a single location for a metric" in {

    lazy val directory = new RAMDirectory()

    implicit val writer = new IndexWriter(directory, new IndexWriterConfig(new StandardAnalyzer))

    val metadataIndex = new LocationIndex(directory)

    (1 to 10).foreach { i =>
      val testData = Location(Coordinates("db", "namespace", s"metric_0"), s"node_0", i - 1, i)
      metadataIndex.write(testData)
    }
    writer.close()

    val firstMetadata = metadataIndex.getLocationForMetricAtTime("metric_0", 1)

    firstMetadata shouldBe Some(
      Location(Coordinates("db", "namespace", s"metric_0"), s"node_0", 0, 1)
    )

    val intermediateMetadata = metadataIndex.getLocationForMetricAtTime("metric_0", 4)

    intermediateMetadata shouldBe Some(
      Location(Coordinates("db", "namespace", s"metric_0"), s"node_0", 3, 4)
    )

    val lastMetadata = metadataIndex.getLocationForMetricAtTime("metric_0", 10)

    lastMetadata shouldBe Some(
      Location(Coordinates("db", "namespace", s"metric_0"), s"node_0", 9, 10)
    )
  }

}
