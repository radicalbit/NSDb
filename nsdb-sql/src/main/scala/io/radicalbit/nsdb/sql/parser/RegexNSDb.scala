package io.radicalbit.nsdb.sql.parser

trait RegexNSDb {
  val metric    = """(^[a-zA-Z][a-zA-Z0-9_]*)""".r
}
