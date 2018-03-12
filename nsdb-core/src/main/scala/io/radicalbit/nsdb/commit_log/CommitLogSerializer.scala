package io.radicalbit.nsdb.commit_log

import io.radicalbit.nsdb.commit_log.CommitLogWriterActor.CommitLogEntry

trait CommitLogSerializer {

  def deserialize(entry: Array[Byte]): CommitLogEntry

  def serialize(entry: CommitLogEntry): Array[Byte]

}
