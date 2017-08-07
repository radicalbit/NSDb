package io.radicalbit.nsdb.commit_log

trait CommitLogSerializer {

  def deserialize(entry: Array[Byte]): InsertNewEntry

  def serialize(entry: InsertNewEntry): Array[Byte]

}
