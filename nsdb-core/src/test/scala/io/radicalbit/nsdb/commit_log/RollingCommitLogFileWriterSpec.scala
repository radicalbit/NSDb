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

  //  val output = Array[Byte](1, 2, 3, 4, 3, 42, 6, 21, 87, 43, 21, 43, 76, 1, 97, 9, 15)
  //
  //  private val serStub = new CommitLogSerializer {
  //    override def deserialize(entry: Array[Byte]): InsertNewEntry = ???
  //    override def serialize(entry: InsertNewEntry): Array[Byte]   = output
  //  }
  //
  //  val path      = "/tmp/commit.log"
  //  val maxSize = 5000
  //  val logWriter = new RollingCommitLogFileWriter
  //
  //  "A FileCommitLogWriter" when {
  //    "receive a single entry" should {
  //      "stored it succesfully" in {
  //
  ////        logWriter.write(0L, "", Map.empty[String, JSerializable])
  //      }
  //    }
  //  }

  "A commit log file name" when {

    val prefix            = "DummyFileNamePrefix"
    val fileNameSeparator = RollingCommitLogFileWriter.FileNameSeparator

    def name(counter: String) = s"$prefix$fileNameSeparator$counter"

    "starting from an empty commit log directory" should {
      "use the correct name" in {

        val files = List.empty[String]
        val nextFileName = RollingCommitLogFileWriter.nextFileName(fileNamePrefix = prefix,
                                                                   fileNameSeparator = fileNameSeparator,
                                                                   fileNames = files)
        nextFileName should be(name("0"))
      }
    }

    "starting from a commit log directory having an existing log" should {
      "use the correct name" in {
        val files = List(name("0"))
        val nextFileName = RollingCommitLogFileWriter.nextFileName(fileNamePrefix = prefix,
                                                                   fileNameSeparator = fileNameSeparator,
                                                                   fileNames = files)
        nextFileName should be(name("1"))
      }
    }

    "starting from a commit log directory having few ordered existing log" should {
      "use the correct name" in {
        val files = List(name("0"), name("1"), name("2"), name("3"))
        val nextFileName = RollingCommitLogFileWriter.nextFileName(fileNamePrefix = prefix,
                                                                   fileNameSeparator = fileNameSeparator,
                                                                   fileNames = files)
        nextFileName should be(name("4"))
      }
    }

    "starting from a commit log directory having few unordered existing log" should {
      "use the correct name" in {
        val nextFileName = RollingCommitLogFileWriter.nextFileName(fileNamePrefix = prefix,
                                                                   fileNameSeparator = fileNameSeparator,
                                                                   fileNames =
                                                                     List(name("3"), name("2"), name("0"), name("1")))
        nextFileName should be(name("4"))

        val nextFileName1 =
          RollingCommitLogFileWriter.nextFileName(fileNamePrefix = prefix,
                                                  fileNameSeparator = fileNameSeparator,
                                                  fileNames =
                                                    List(name("3"), name("0"), name("2"), name("1"), name("111")))
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
        val nextFileName = RollingCommitLogFileWriter.nextFileName(fileNamePrefix = prefix,
                                                                   fileNameSeparator = fileNameSeparator,
                                                                   fileNames = files)
        nextFileName should be(name("4"))
      }
    }
  }
}
