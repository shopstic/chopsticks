package dev.chopsticks.kvdb.util

import dev.chopsticks.kvdb.proto.KvdbOperationException

import scala.util.control.NoStackTrace

sealed abstract class KvdbException(val exceptionType: KvdbOperationException.ExceptionType, msg: String)
    extends RuntimeException(msg)
    with NoStackTrace

object KvdbException {
  final case class SeekFailure(msg: String)
      extends KvdbException(KvdbOperationException.ExceptionType.SEEK_FAILURE, msg)

  final case class UnoptimizedKvdbOperationException(msg: String)
      extends KvdbException(KvdbOperationException.ExceptionType.UNOPTIMIZED_OPERATION, msg)

  final case class InvalidKvdbColumnFamilyException(msg: String)
      extends KvdbException(KvdbOperationException.ExceptionType.INVALID_COLUMN_FAMILY, msg)

  final case class InvalidKvdbArgumentException(msg: String)
      extends KvdbException(KvdbOperationException.ExceptionType.INVALID_ARGUMENT, msg)

  final case class UnsupportedKvdbOperationException(msg: String)
      extends KvdbException(KvdbOperationException.ExceptionType.UNSUPPORTED_OPERATION, msg)

  final case class KvdbAlreadyClosedException(msg: String)
      extends KvdbException(KvdbOperationException.ExceptionType.ALREADY_CLOSED, msg)
}
