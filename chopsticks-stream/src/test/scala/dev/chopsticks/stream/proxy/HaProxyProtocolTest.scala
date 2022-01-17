package dev.chopsticks.stream.proxy

import akka.stream.scaladsl.{Keep, Sink, Source}
import akka.testkit.ImplicitSender
import akka.util.ByteString
import dev.chopsticks.stream.proxy.HaProxyProtocol.{
  HaProxyAddresses,
  HaProxyMessage,
  HaProxyProtocolError,
  HaProxyTransportProtocol
}
import dev.chopsticks.testkit.{AkkaTestKit, AkkaTestKitAutoShutDown}
import dev.chopsticks.util.Hex
import org.scalatest.matchers.must.Matchers
import org.scalatest.wordspec.AnyWordSpecLike

import java.net.InetAddress
import scala.concurrent.{Await, Future}
import scala.concurrent.duration._

final class HaProxyProtocolTest
    extends AkkaTestKit
    with ImplicitSender
    with AnyWordSpecLike
    with Matchers
    with AkkaTestKitAutoShutDown {

  implicit private class FutureOps[A](val future: Future[A]) {
    def await: A = Await.result(future, 5.seconds)
  }

  "decode HaProxyMessage with ipv4 proxied addresses" in {
    val source = Source(List(
      ByteString.fromArray(Hex.decode("0D0A0D0A000D0A515549540A2111000C0A0101011402020203E807D0")),
      ByteString.fromString("TCP MESSAGE"),
      ByteString.fromString("ANOTHER MESSAGE")
    ))
    val (futureHaMessage, futureResult) = source
      .viaMat(HaProxyProtocol.decodingFlow)(Keep.right)
      .map(_.utf8String)
      .toMat(Sink.seq)(Keep.both)
      .run()

    val haMessage = futureHaMessage.await
    haMessage mustEqual Some(HaProxyMessage(
      HaProxyTransportProtocol.Stream,
      HaProxyAddresses.HaProxyIpv4Addresses(
        src = HaProxyAddresses.HaProxyIpv4Address(
          addr = InetAddress.getByName("10.1.1.1"),
          port = 1000
        ),
        dst = HaProxyAddresses.HaProxyIpv4Address(
          addr = InetAddress.getByName("20.2.2.2"),
          port = 2000
        )
      )
    ))

    futureResult.await mustEqual Seq("TCP MESSAGE", "ANOTHER MESSAGE")
  }

  "decode HaProxyMessage with ipv6 proxied addresses" in {
    val source = Source(List(
      ByteString.fromArray(Hex.decode(
        "0D0A0D0A000D0A515549540A2122002400000000000000000000FFFFC000020120010DB80000000000000000000000680B5505DC"
      )),
      ByteString.fromString("MSG"),
      ByteString.fromString("ANOTHER MESSAGE")
    ))
    val (futureHaMessage, futureResult) = source
      .viaMat(HaProxyProtocol.decodingFlow)(Keep.right)
      .map(_.utf8String)
      .toMat(Sink.seq)(Keep.both)
      .run()

    val haMessage = futureHaMessage.await
    haMessage mustEqual Some(HaProxyMessage(
      HaProxyTransportProtocol.Dgram,
      HaProxyAddresses.HaProxyIpv6Addresses(
        src = HaProxyAddresses.HaProxyIpv6Address(
          addr = InetAddress.getByName("192.0.2.1"),
          port = 2901
        ),
        dst = HaProxyAddresses.HaProxyIpv6Address(
          addr = InetAddress.getByName("2001:db8:0:0:0:0:0:68"),
          port = 1500
        )
      )
    ))

    futureResult.await mustEqual Seq("MSG", "ANOTHER MESSAGE")
  }

  "fail stream when proxy message is incorrect, even though the signature is correct" in {
    val source = Source(List(
      ByteString.fromArray(Hex.decode("0D0A0D0A000D0A515549540A2511000C0A0101011402020203E807D0")),
      ByteString.fromString("TCP MESSAGE"),
      ByteString.fromString("ANOTHER MESSAGE")
    ))
    val (futureHaMessage, futureResult) = source
      .viaMat(HaProxyProtocol.decodingFlow)(Keep.right)
      .map(_.utf8String)
      .toMat(Sink.seq)(Keep.both)
      .run()

    an[HaProxyProtocolError] must be thrownBy {
      futureHaMessage.await
    }

    an[HaProxyProtocolError] must be thrownBy {
      futureResult.await
    }
  }

  "decode HaProxyMessage when the proxy message arrives in chunks" in {
    val source = Source(List(
      ByteString.fromArray(Hex.decode("0D0A0D0A000D0A515549540A2111000C0A0101011402")),
      ByteString.fromArray(Hex.decode("020203E807D0")),
      ByteString.fromString("TCP MESSAGE"),
      ByteString.fromString("ANOTHER MESSAGE")
    ))
    val (futureHaMessage, futureResult) = source
      .viaMat(HaProxyProtocol.decodingFlow)(Keep.right)
      .map(_.utf8String)
      .toMat(Sink.seq)(Keep.both)
      .run()

    val haMessage = futureHaMessage.await
    haMessage mustEqual Some(HaProxyMessage(
      HaProxyTransportProtocol.Stream,
      HaProxyAddresses.HaProxyIpv4Addresses(
        src = HaProxyAddresses.HaProxyIpv4Address(
          addr = InetAddress.getByName("10.1.1.1"),
          port = 1000
        ),
        dst = HaProxyAddresses.HaProxyIpv4Address(
          addr = InetAddress.getByName("20.2.2.2"),
          port = 2000
        )
      )
    ))

    futureResult.await mustEqual Seq("TCP MESSAGE", "ANOTHER MESSAGE")
  }

  "decode HaProxyMessage when the proxy message arrives in chunks and new packet arrives along with the last part of the proxy message" in {
    val source = Source(List(
      ByteString.fromArray(Hex.decode("0D0A0D0A000D0A515549540A2111000C0A0101011402")),
      ByteString.fromArray(Hex.decode("020203E807D0") ++ ByteString.fromString("TCP MESSAGE").toArrayUnsafe()),
      ByteString.fromString("ANOTHER MESSAGE")
    ))
    val (futureHaMessage, futureResult) = source
      .viaMat(HaProxyProtocol.decodingFlow)(Keep.right)
      .map(_.utf8String)
      .toMat(Sink.seq)(Keep.both)
      .run()

    val haMessage = futureHaMessage.await
    haMessage mustEqual Some(HaProxyMessage(
      HaProxyTransportProtocol.Stream,
      HaProxyAddresses.HaProxyIpv4Addresses(
        src = HaProxyAddresses.HaProxyIpv4Address(
          addr = InetAddress.getByName("10.1.1.1"),
          port = 1000
        ),
        dst = HaProxyAddresses.HaProxyIpv4Address(
          addr = InetAddress.getByName("20.2.2.2"),
          port = 2000
        )
      )
    ))

    futureResult.await mustEqual Seq("TCP MESSAGE", "ANOTHER MESSAGE")
  }

  "decode port numbers above 32767" in {
    val source = Source(List(
      ByteString
        .fromString(
          "DQoNCgANClFVSVQKIREAVGRAC2oKeySW8cEK1wMABCZDxm4EAD4AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=="
        )
        .decodeBase64
    ))
    val (futureHaMessage, futureResult) = source
      .wireTap(bs => println(Hex.encode(bs.toArrayUnsafe())))
      .viaMat(HaProxyProtocol.decodingFlow)(Keep.right)
      .map(bs => Hex.encode(bs.toArray))
      .toMat(Sink.seq)(Keep.both)
      .run()

    val haMessage = futureHaMessage.await
    haMessage mustEqual Some(HaProxyMessage(
      HaProxyTransportProtocol.Stream,
      HaProxyAddresses.HaProxyIpv4Addresses(
        src = HaProxyAddresses.HaProxyIpv4Address(
          addr = InetAddress.getByName("100.64.11.106"),
          port = 61889
        ),
        dst = HaProxyAddresses.HaProxyIpv4Address(
          addr = InetAddress.getByName("10.123.36.150"),
          port = 2775
        )
      )
    ))

    futureResult.await mustEqual Seq("000000000000000000000000000000000000000000000000000000000000000000000000000000")
  }

  "messages that arrive before HaProxyMessage should be passed-through and materialized value should equal to None" in {
    val source = Source(List(
      ByteString.fromString("TCP MESSAGE1"),
      ByteString.fromString("TCP MESSAGE2")
    ))
    val (futureHaMessage, futureResult) = source
      .viaMat(HaProxyProtocol.decodingFlow)(Keep.right)
      .map(_.utf8String)
      .toMat(Sink.seq)(Keep.both)
      .run()

    val haMessage = futureHaMessage.await
    haMessage mustEqual None

    futureResult.await mustEqual Seq("TCP MESSAGE1", "TCP MESSAGE2")
  }

}
