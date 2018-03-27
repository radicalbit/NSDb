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
