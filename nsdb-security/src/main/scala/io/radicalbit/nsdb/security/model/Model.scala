package io.radicalbit.nsdb.security.model

/**
  * Trait for a Rpc request related to a database.
  */
trait Db {
  val db: String
}

/**
  * Trait for a Rpc request related to a namespace.
  */
trait Namespace extends Db {
  val namespace: String
}

/**
  * Trait for a Rpc request related to a metric.
  */
trait Metric extends Namespace {
  val metric: String
}
