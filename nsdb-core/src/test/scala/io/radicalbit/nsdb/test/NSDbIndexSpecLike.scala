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

package io.radicalbit.nsdb.test

import io.radicalbit.nsdb.index.TimeSeriesIndex
import org.apache.lucene.index.IndexWriter
import org.apache.lucene.store.MMapDirectory
import org.scalatest.{BeforeAndAfter, BeforeAndAfterAll, TestSuite}

import java.nio.file.Paths
import java.util.UUID

/**
  * Provides utility methods to facilitate and segregate test against NSDb indices.
  */
trait NSDbIndexSpecLike { this: TestSuite =>

  protected lazy val rootPath = Paths.get("target", "test_index")
  lazy val basePath           = Paths.get(rootPath.toString, this.getClass.getSimpleName, UUID.randomUUID().toString)

}

/**
  * Provides a base structure amd utility methods for tests against NSDb time series indices.
  */
trait NSDbTimeSeriesIndexSpecLike extends NSDbIndexSpecLike with BeforeAndAfter with BeforeAndAfterAll {
  this: TestSuite =>

  lazy val timeSeriesIndex              = new TimeSeriesIndex(new MMapDirectory(basePath))
  implicit lazy val writer: IndexWriter = timeSeriesIndex.getWriter

  before {
    timeSeriesIndex.deleteAll()
    writer.commit()
    timeSeriesIndex.refresh()
  }

  override def afterAll(): Unit = {
    writer.close()
    timeSeriesIndex.close()
  }

  def commit(): Unit = {
    writer.commit()
    timeSeriesIndex.refresh()
  }
}
