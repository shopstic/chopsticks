package dev.chopsticks.kvdb.codec

import com.google.protobuf.ByteString
import dev.chopsticks.util.Hex

import java.nio.charset.StandardCharsets
import scala.collection.immutable.ArraySeq

@main
def testPacking(): Unit =
  showStr("test")
  showStr("\u0000")

  def showStr(string: String): Unit =
    var tuple = new com.apple.foundationdb.tuple.Tuple()
    //  tuple = tuple.add("test".getBytes(StandardCharsets.UTF_8))
    tuple = tuple.add(string.getBytes(StandardCharsets.UTF_8))
    val encoded = tuple.pack()
    val hex = Hex.encode(encoded)
    val byteString = ByteString.copyFrom(encoded)
    val arraySeq: ArraySeq[Byte] = scala.collection.immutable.ArraySeq.from(encoded)
    println(s"Showing           : $string")
    println(s"Tuple      [tuple]: ${tuple.toString}")
    println(s"Tuple        [hex]: $hex")
    println(s"Tuple [bytestring]: $byteString")
    println(s"Tuple   [arrayseq]: $arraySeq")
    println()
