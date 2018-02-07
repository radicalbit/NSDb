package io.radicalbit.nsdb.commit_log

trait CommitLogSerializer {

  def deserialize(entry: Array[Byte]): CommitLogEntry

  def serialize(entry: CommitLogEntry): Array[Byte]

}
