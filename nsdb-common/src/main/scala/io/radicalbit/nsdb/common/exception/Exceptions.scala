/*
 * Copyright 2018-2020 Radicalbit S.r.l.
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
  * Trait for NSDb Exceptions. It extends [[NoStackTrace]] for efficiency purpose.
  */
sealed trait NSDbException extends RuntimeException with NoStackTrace

class InvalidStatementException(val message: String) extends NSDbException
class TypeNotSupportedException(val message: String) extends NSDbException
class NsdbSecurityException(val message: String)     extends NSDbException
class TooManyRetriesException(val message: String)   extends NSDbException
class InvalidNodeIdException(address: String)        extends NSDbException
