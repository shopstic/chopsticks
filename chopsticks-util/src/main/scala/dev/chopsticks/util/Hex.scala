package dev.chopsticks.util

import java.nio.charset.StandardCharsets

object Hex {
  private val HexArray = "0123456789ABCDEF".getBytes(StandardCharsets.US_ASCII)

  // use HexFormat once we switch to JDK 17
  def encode(bytes: Array[Byte]): String = {
    val hexChars = new Array[Byte](bytes.length * 2)
    var i = 0
    while (i < bytes.length) {
      val v = bytes(i) & 0xFF
      hexChars(i * 2) = HexArray(v >>> 4)
      hexChars(i * 2 + 1) = HexArray(v & 0x0F)
      i += 1
    }
    new String(hexChars, StandardCharsets.UTF_8)
  }

  // use HexFormat once we switch to JDK 17
  def decode(s: String): Array[Byte] = {
    assert(s.length % 2 == 0, "s must be an even-length string")
    val len = s.length
    val data = new Array[Byte](len / 2)
    var i = 0
    while (i < len) {
      val highestBits = Character.digit(s.charAt(i), 16) << 4
      val lowestBits = Character.digit(s.charAt(i + 1), 16)
      data(i / 2) = (highestBits | lowestBits).toByte
      i += 2
    }
    data
  }
}
