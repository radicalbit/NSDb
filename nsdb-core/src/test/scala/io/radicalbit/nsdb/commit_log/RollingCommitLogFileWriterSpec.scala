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

package io.radicalbit.nsdb.commit_log

import java.io.{File, FileOutputStream}
import java.nio.file.Paths
import java.util.concurrent.TimeUnit

import akka.actor.ActorSystem
import akka.testkit.{ImplicitSender, TestKit, TestProbe}
import io.radicalbit.nsdb.commit_log.CommitLogWriterActor.{RejectedEntryAction, WriteToCommitLog}
import io.radicalbit.nsdb.commit_log.RollingCommitLogFileWriter.ForceRolling
import io.radicalbit.nsdb.model.Location
import io.radicalbit.nsdb.test.NSDbSpecLike
import org.scalatest.BeforeAndAfter

import scala.concurrent.duration.FiniteDuration

class RollingCommitLogFileWriterSpec
    extends TestKit(ActorSystem("radicaldb-test"))
    with NSDbSpecLike
    with ImplicitSender
    with BeforeAndAfter
    with CommitLogSpec {

  private val prefix            = RollingCommitLogFileWriter.fileNamePrefix
  private val fileNameSeparator = RollingCommitLogFileWriter.fileNameSeparator

  lazy val passivateAfter =
    FiniteDuration(system.settings.config.getDuration("nsdb.commit-log.passivate-after").toNanos, TimeUnit.NANOSECONDS)

  before {
    new File(directory).mkdirs()

    val existingFiles = Option(Paths.get(directory).toFile.listFiles())
      .map(_.toSet)
      .getOrElse(Set.empty)

    existingFiles.foreach(f => f.delete)
  }

  "A rolling commit log writer " when {
    "starting" should {
      "check and delete old files when they are balanced" in {
        val firstFileName = RollingCommitLogFileWriter.nextFileName(db, namespace, metric, Seq.empty)

        val secondFileName = RollingCommitLogFileWriter.nextFileName(db, namespace, metric, Seq(firstFileName))

        val balancedFile   = new File(s"$directory/$firstFileName")
        val balancedFileOS = new FileOutputStream(balancedFile, true)

        balancedEntrySeq foreach { entry =>
          balancedFileOS.write(serializer.serialize(entry))
        }

        balancedFileOS.flush()
        balancedFileOS.close()

        val unbalancedFile   = new File(s"$directory/$secondFileName")
        val unbalancedFileOS = new FileOutputStream(unbalancedFile, true)

        unbalancedEntrySeq foreach { entry =>
          unbalancedFileOS.write(serializer.serialize(entry))
        }

        unbalancedFileOS.flush()
        unbalancedFileOS.close()

        system.actorOf(RollingCommitLogFileWriter.props(db, namespace, metric))

        awaitAssert {
          val existingFiles = Option(Paths.get(directory).toFile.list())
            .map(_.toSet)
            .getOrElse(Set.empty)

          existingFiles.size shouldBe 1
          existingFiles shouldBe Set(secondFileName)
        }
      }

      "check and delete old files when they are not balanced" in {
        val firstFileName  = RollingCommitLogFileWriter.nextFileName(db, namespace, metric, Seq.empty)
        val secondFileName = RollingCommitLogFileWriter.nextFileName(db, namespace, metric, Seq(firstFileName))

        val unbalancedFile   = new File(s"$directory/$secondFileName")
        val unbalancedFileOS = new FileOutputStream(unbalancedFile, true)

        unbalancedEntrySeq foreach { entry =>
          unbalancedFileOS.write(serializer.serialize(entry))
        }

        unbalancedFileOS.flush()
        unbalancedFileOS.close()

        val rolling = system.actorOf(RollingCommitLogFileWriter.props(db, namespace, metric))

        rolling ! WriteToCommitLog(db, namespace, metric, 1, RejectedEntryAction(bit1), Location(metric, "node", 0, 0))

        rolling ! ForceRolling

        awaitAssert {
          val existingFiles = Option(Paths.get(directory).toFile.list())
            .map(_.toSet)
            .getOrElse(Set.empty)

          existingFiles.size shouldBe 1
          new File(s"$directory/${existingFiles.head}").length() shouldBe 0
        }
      }

      "passivate itself after a period of inactivity" in {
        val rolling = system.actorOf(RollingCommitLogFileWriter.props(db, namespace, metric))

        val probe = TestProbe()
        probe.watch(rolling)
        probe.expectTerminated(rolling, passivateAfter)
      }
    }
  }

  "A commit log file name" when {

    def name(counter: String) =
      s"$prefix$fileNameSeparator$db$fileNameSeparator$namespace$fileNameSeparator$metric$fileNameSeparator$counter"

    "starting from an empty commit log directory" should {
      "use the correct name" in {

        val files = List.empty[String]
        val nextFileName =
          RollingCommitLogFileWriter.nextFileName(db = db, namespace = namespace, metric = metric, fileNames = files)
        nextFileName should be(name("0"))
      }
    }

    "starting from a commit log directory having an existing log" should {
      "use the correct name" in {
        val files = List(name("0"))
        val nextFileName =
          RollingCommitLogFileWriter.nextFileName(db = db, namespace = namespace, metric = metric, fileNames = files)
        nextFileName should be(name("1"))
      }
    }

    "starting from a commit log directory having few ordered existing log" should {
      "use the correct name" in {
        val files = List(name("0"), name("1"), name("2"), name("3"))
        val nextFileName =
          RollingCommitLogFileWriter.nextFileName(db = db, namespace = namespace, metric = metric, fileNames = files)
        nextFileName should be(name("4"))
      }
    }

    "starting from a commit log directory having few unordered existing log" should {
      "use the correct name" in {
        val nextFileName = RollingCommitLogFileWriter.nextFileName(
          db = db,
          namespace = namespace,
          metric = metric,
          fileNames = List(name("3"), name("2"), name("0"), name("1"))
        )
        nextFileName should be(name("4"))

        val nextFileName1 =
          RollingCommitLogFileWriter.nextFileName(
            db = db,
            namespace = namespace,
            metric = metric,
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
        val nextFileName =
          RollingCommitLogFileWriter.nextFileName(db = db, namespace = namespace, metric = metric, fileNames = files)
        nextFileName should be(name("4"))
      }
    }
  }
}
