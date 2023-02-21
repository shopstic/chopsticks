package dev.chopsticks.kvdb.util

import dev.chopsticks.kvdb.proto.KvdbOperationException

import scala.util.control.NoStackTrace

enum KvdbException(val exceptionType: KvdbOperationException.ExceptionType, msg: String)
    extends RuntimeException(msg) with NoStackTrace:

  case SeekFailure(msg: String) extends KvdbException(KvdbOperationException.ExceptionType.SEEK_FAILURE, msg)

  case UnoptimizedKvdbOperationException(msg: String)
      extends KvdbException(KvdbOperationException.ExceptionType.UNOPTIMIZED_OPERATION, msg)

  case InvalidKvdbColumnFamilyException(msg: String)
      extends KvdbException(KvdbOperationException.ExceptionType.INVALID_COLUMN_FAMILY, msg)

  case InvalidKvdbArgumentException(msg: String)
      extends KvdbException(KvdbOperationException.ExceptionType.INVALID_ARGUMENT, msg)

  case UnsupportedKvdbOperationException(msg: String)
      extends KvdbException(KvdbOperationException.ExceptionType.UNSUPPORTED_OPERATION, msg)

  case KvdbAlreadyClosedException(msg: String)
      extends KvdbException(KvdbOperationException.ExceptionType.ALREADY_CLOSED, msg)

  case ConditionalTransactionFailedException(msg: String)
      extends KvdbException(KvdbOperationException.ExceptionType.CONDITIONAL_TRANSACTION_FAILED, msg)

end KvdbException
