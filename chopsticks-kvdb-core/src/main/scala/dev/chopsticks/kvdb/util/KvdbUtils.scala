package dev.chopsticks.kvdb.util

import com.google.protobuf.{ByteString => ProtoByteString}
import dev.chopsticks.kvdb.proto.KvdbKeyConstraint.Operator
import dev.chopsticks.kvdb.proto.{KvdbKeyConstraint, KvdbKeyRange}
import pureconfig.{PascalCase, SnakeCase}

object KvdbUtils {
  def resumeRange(range: KvdbKeyRange, lastKey: Array[Byte], lastKeyDisplay: String): KvdbKeyRange = {
    if (lastKey.isEmpty) range
    else {
      val rangeTo = range.to
      val tail = if (rangeTo.size == 1 && rangeTo.head.operator == Operator.LAST) Nil else rangeTo
      range.copy(from = KvdbKeyConstraint(Operator.GREATER, ProtoByteString.copyFrom(lastKey), lastKeyDisplay) :: tail)
    }
  }

  def deriveColumnFamilyId(clazz: Class[_]): String = {
    val enclosingLength = Option(clazz.getEnclosingClass).map(_.getName.length).getOrElse(0)
    val classFullName = clazz.getName.drop(enclosingLength)
    val className = classFullName
      .drop(classFullName.lastIndexOf(".") + 1)
      .replaceAllLiterally("$", "")
    SnakeCase.fromTokens(PascalCase.toTokens(className))
  }
}
