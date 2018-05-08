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

package io.radicalbit.nsdb.common.statement

/**
  * Trait for commands.
  */
sealed trait CommandStatement extends NSDBStatement

/**
  * CLI command to retrieve the namespaces present in the database.
  */
object ShowNamespaces extends CommandStatement

/**
  * CLI command to use the given namespace.
  */
case class UseNamespace(namespace: String) extends CommandStatement

/**
  * CLI command to retrieve the metrics present in the database for the given namespace.
  */
case class ShowMetrics(db: String, namespace: String) extends CommandStatement

/**
  * CLI command to describe the given metric for the give namespace.
  */
case class DescribeMetric(db: String, namespace: String, metric: String) extends CommandStatement
