package io.radicalbit.nsdb.security.model

trait Db {
  val db: String
}

trait Namespace extends Db {
  val namespace: String
}

trait Metric extends Namespace {
  val metric: String
}
