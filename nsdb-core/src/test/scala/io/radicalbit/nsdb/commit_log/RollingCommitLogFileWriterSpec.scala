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

package io.radicalbit.nsdb.commit_log

import akka.actor.ActorSystem
import akka.testkit.{ImplicitSender, TestKit}
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpecLike}

class RollingCommitLogFileWriterSpec
    extends TestKit(ActorSystem("radicaldb-test"))
    with ImplicitSender
    with WordSpecLike
    with Matchers
    with BeforeAndAfterAll {

  "A commit log file name" when {

    val db                = "database"
    val namespace         = "namespace"
    val prefix            = "DummyFileNamePrefix"
    val fileNameSeparator = RollingCommitLogFileWriter.FileNameSeparator

    def name(counter: String) = s"$prefix$fileNameSeparator$db$fileNameSeparator$namespace$fileNameSeparator$counter"

    "starting from an empty commit log directory" should {
      "use the correct name" in {

        val files = List.empty[String]
        val nextFileName = RollingCommitLogFileWriter.nextFileName(db = db,
                                                                   namespace = namespace,
                                                                   fileNamePrefix = prefix,
                                                                   fileNameSeparator = fileNameSeparator,
                                                                   fileNames = files)
        nextFileName should be(name("0"))
      }
    }

    "starting from a commit log directory having an existing log" should {
      "use the correct name" in {
        val files = List(name("0"))
        val nextFileName = RollingCommitLogFileWriter.nextFileName(db = db,
                                                                   namespace = namespace,
                                                                   fileNamePrefix = prefix,
                                                                   fileNameSeparator = fileNameSeparator,
                                                                   fileNames = files)
        nextFileName should be(name("1"))
      }
    }

    "starting from a commit log directory having few ordered existing log" should {
      "use the correct name" in {
        val files = List(name("0"), name("1"), name("2"), name("3"))
        val nextFileName = RollingCommitLogFileWriter.nextFileName(db = db,
                                                                   namespace = namespace,
                                                                   fileNamePrefix = prefix,
                                                                   fileNameSeparator = fileNameSeparator,
                                                                   fileNames = files)
        nextFileName should be(name("4"))
      }
    }

    "starting from a commit log directory having few unordered existing log" should {
      "use the correct name" in {
        val nextFileName = RollingCommitLogFileWriter.nextFileName(db = db,
                                                                   namespace = namespace,
                                                                   fileNamePrefix = prefix,
                                                                   fileNameSeparator = fileNameSeparator,
                                                                   fileNames =
                                                                     List(name("3"), name("2"), name("0"), name("1")))
        nextFileName should be(name("4"))

        val nextFileName1 =
          RollingCommitLogFileWriter.nextFileName(
            db = db,
            namespace = namespace,
            fileNamePrefix = prefix,
            fileNameSeparator = fileNameSeparator,
            fileNames = List(name("3"), name("0"), name("2"), name("1"), name("111"))
          )
        nextFileName1 should be(name("112"))
      }
    }

    "starting from a commit log directory having few unexpected files" should {
      "use the correct name" in {
        val files = List("AAAA",
                         name("00003"),
                         name("00001"),
                         "-34232fdsfd",
                         name("-4523432"),
                         name("00002"),
                         "jdksjadlkajdlsa",
                         "_______",
                         "_dada")
        val nextFileName = RollingCommitLogFileWriter.nextFileName(db = db,
                                                                   namespace = namespace,
                                                                   fileNamePrefix = prefix,
                                                                   fileNameSeparator = fileNameSeparator,
                                                                   fileNames = files)
        nextFileName should be(name("4"))
      }
    }
  }
}
