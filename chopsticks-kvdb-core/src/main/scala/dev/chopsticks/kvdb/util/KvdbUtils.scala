package dev.chopsticks.kvdb.util

import java.util

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

  private def rstrip(input: Array[Byte], target: Byte): Array[Byte] = {
    val lastIndex = input.length - 1
    var i = lastIndex

    while (i >= 0 && input(i) == target) {
      i = i - 1
    }

    util.Arrays.copyOfRange(input, 0, i + 1)
  }

  /** From FDB ByteArrayUtil.java Computes the first key that would sort outside the range prefixed by {@code key}.
    * {@code key} must be non-null, and contain at least some character this is not {@code \xFF} (255).
    *
    * @param key
    *   prefix key
    * @return
    *   a newly created byte array
    */
  def strinc(key: Array[Byte]): Array[Byte] = {
    val copy = rstrip(key, 0xFF.toByte)
    if (copy.length == 0) throw new IllegalArgumentException("No key beyond supplied prefix")
    // Since rstrip makes sure the last character is not \xff, we can be sure
    //  we're able to add 1 to it without overflow.
    copy(copy.length - 1) = (copy(copy.length - 1) + 1).toByte
    copy
  }

  def deriveColumnFamilyId(clazz: Class[_]): String = {
    val enclosingLength = Option(clazz.getEnclosingClass).map(_.getName.length).getOrElse(0)
    val classFullName = clazz.getName.drop(enclosingLength)
    val className = classFullName
      .drop(classFullName.lastIndexOf(".") + 1)
      .replace("$", "")
    SnakeCase.fromTokens(PascalCase.toTokens(className))
  }
}
