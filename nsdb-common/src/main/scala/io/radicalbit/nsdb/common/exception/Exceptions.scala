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

package io.radicalbit.nsdb.common.exception

import scala.util.control.NoStackTrace

/**
  * Trait for Nsdb Exceptions. It extends [[NoStackTrace]] for efficiency purpose.
  */
sealed trait NsdbException extends RuntimeException with NoStackTrace

class NsdbConnectionException(val message: String)   extends NsdbException
class MetricNotFoundException(val message: String)   extends NsdbException
class InvalidStatementException(val message: String) extends NsdbException
class TypeNotSupportedException(val message: String) extends NsdbException
class NsdbSecurityException(val message: String)     extends NsdbException
