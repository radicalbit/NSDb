package io.radicalbit.nsdb.common.exception

sealed trait NsdbException

abstract class NsdbRuntimeException(val message: String) extends RuntimeException with NsdbException
class NsdbConnectionException(val message: String)       extends RuntimeException with NsdbException
class MetricNotFoundException(val message: String)       extends RuntimeException with NsdbException
class InvalidStatementException(val message: String)     extends RuntimeException with NsdbException
class TypeNotSupportedException(val message: String)     extends RuntimeException with NsdbException
class NsdbSecurityException(val message: String)         extends RuntimeException with NsdbException
