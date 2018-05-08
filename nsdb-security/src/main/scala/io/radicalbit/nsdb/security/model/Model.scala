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
