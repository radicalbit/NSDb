package io.radicalbit.nsdb.commit_log

import io.radicalbit.commit_log.InsertNewEntry

trait CommitLogSerializer {

  def deserialize(entry: Array[Byte]): InsertNewEntry

  def serialize(entry: InsertNewEntry): Array[Byte]

}
