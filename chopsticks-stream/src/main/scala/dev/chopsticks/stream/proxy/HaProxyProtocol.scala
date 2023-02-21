/*
package dev.chopsticks.stream.proxy

import akka.stream.{Attributes, FlowShape, Inlet, Outlet}

import java.nio.ByteOrder
import java.util.Base64
import akka.stream.scaladsl.{BidiFlow, Flow, Keep}
import akka.stream.stage.{GraphStageLogic, GraphStageWithMaterializedValue, InHandler, OutHandler}
import akka.util.{ByteIterator, ByteString}
import dev.chopsticks.stream.proxy.HaProxyProtocol.HaProxyMessage

import java.net.InetAddress
import scala.collection.compat.immutable.ArraySeq
import scala.concurrent.{Future, Promise}

object HaProxyProtocol {
  final case class HaProxyMessage(
    transportProtocol: HaProxyTransportProtocol,
    proxyAddresses: HaProxyAddresses
  )

  sealed abstract class HaProxyAddresses extends Product with Serializable

  object HaProxyAddresses {
    val Ipv6AddressLength = 16
    val UnixAddressLength = 108

    final case class HaProxyIpv4Address(addr: InetAddress, port: Int)
    final case class HaProxyIpv6Address(addr: InetAddress, port: Int)
    final case class HaProxyUnixAddress(addr: ArraySeq[Byte])

    sealed abstract class HaProxySpecifiedAddresses extends HaProxyAddresses {
      type AddressType <: Product with Serializable

      def src: AddressType
      def dst: AddressType
    }

    final case object HaProxyUnspecifiedAddresses extends HaProxyAddresses

    final case class HaProxyIpv4Addresses(src: HaProxyIpv4Address, dst: HaProxyIpv4Address)
        extends HaProxySpecifiedAddresses {
      override type AddressType = HaProxyIpv4Address
    }
    final case class HaProxyIpv6Addresses(src: HaProxyIpv6Address, dst: HaProxyIpv6Address)
        extends HaProxySpecifiedAddresses {
      override type AddressType = HaProxyIpv6Address
    }
    final case class HaProxyUnixAddresses(src: HaProxyUnixAddress, dst: HaProxyUnixAddress)
        extends HaProxySpecifiedAddresses {
      override type AddressType = HaProxyUnixAddress
    }
  }

  sealed abstract class HaProxyTransportProtocol(val value: Byte) extends Product with Serializable
  object HaProxyTransportProtocol {
    final case object Unspecified extends HaProxyTransportProtocol(0)
    // InverseQuery is not used in EnumResolver, but it's a part of DNS spec
    final case object Stream extends HaProxyTransportProtocol(1) // either TCP or UNIX_STREAM
    final case object Dgram extends HaProxyTransportProtocol(2) // either UDP or UNIX_DGRAM

    val tcpOrUdp: Set[HaProxyTransportProtocol] = Set(Stream, Dgram)
    val values: ArraySeq[HaProxyTransportProtocol] = ArraySeq(Unspecified, Stream, Dgram)
    val byValues: Map[Byte, HaProxyTransportProtocol] = values.iterator.map(v => v.value -> v).toMap
    val byteValuesSet: Set[Byte] = values.iterator.map(_.value).toSet
  }

  final case class HaProxyProtocolError(message: String) extends RuntimeException(message)

  implicit private[proxy] val ProxyProtocolByteOrder = ByteOrder.BIG_ENDIAN

  private[proxy] val HeaderLength = 16

  private[proxy] val V2ProtocolSignature = ArraySeq[Byte](
    byte("0d"),
    byte("0a"),
    byte("0d"),
    byte("0a"),
    byte("00"),
    byte("0d"),
    byte("0a"),
    byte("51"),
    byte("55"),
    byte("49"),
    byte("54"),
    byte("0a")
  )

  private[proxy] val LocalCommand = 0
  private[proxy] val ProxyCommand = 1
  private[proxy] val Commands = Set(LocalCommand, ProxyCommand)

  private[proxy] val AddressFamilyUnspecified = 0
  private[proxy] val AddressFamilyIpV4 = 1
  private[proxy] val AddressFamilyIpV6 = 2
  private[proxy] val AddressFamilyUnix = 3
  private[proxy] val AddressFamilies =
    Set(AddressFamilyUnspecified, AddressFamilyIpV4, AddressFamilyIpV6, AddressFamilyUnix)

  def decodingFlow: Flow[ByteString, ByteString, Future[Option[HaProxyMessage]]] = {
    Flow.fromGraph(new HaProxyDecodingFlow)
  }

  def decodingProtocol: BidiFlow[ByteString, ByteString, ByteString, ByteString, Future[Option[HaProxyMessage]]] = {
    BidiFlow.fromFlowsMat(decodingFlow, Flow[ByteString])(Keep.left)
  }

  private def byte(hexString: String): Byte = {
    val firstDigit = toDigit(hexString.charAt(0))
    val secondDigit = toDigit(hexString.charAt(1))
    ((firstDigit << 4) + secondDigit).toByte
  }

  private def toDigit(hexChar: Char): Int = {
    val digit = Character.digit(hexChar, 16)
    if (digit == -1) {
      throw new IllegalArgumentException(s"Invalid Hexadecimal Character: $hexChar")
    }
    digit
  }

  implicit private[proxy] class ByteIteratorOps(val bi: ByteIterator) extends AnyVal {
    def getUnsignedShort(implicit order: ByteOrder): Int = {
      bi.getShort(order) & 0xFFFF
    }
  }
}

object HaProxyDecoder {
  import HaProxyProtocol._

  def decode(bs: ByteString): HaProxyMessage = {
    val bi = bs.iterator

    expectV2Protocol(bi, bs)

    val protocolVersionAndCommand = bi.getByte
    // take 4 highest bits
    (protocolVersionAndCommand >> 4) match {
      case 2 => ()
      case otherVersion =>
        throw HaProxyProtocolError(
          s"Decoding HaProxyMessage has failed. Expected version 2, " +
            s"but instead got $otherVersion Base64 encoded message: ${base64Message(bs)}"
        )
    }
    // take 4 lowest bits
    val _ = (protocolVersionAndCommand & 15) match {
      case cmd if Commands.contains(cmd) => cmd
      case otherCommand =>
        throw HaProxyProtocolError(
          s"Decoding HaProxyMessage has failed. Incorrect command, expected one of: " +
            s"[${Commands.mkString(", ")}], but instead got ${otherCommand}"
        )
    }

    val addressFamilyAndTransportProtocol = bi.getByte
    // take 4 highest bits
    val addressFamily = (addressFamilyAndTransportProtocol >> 4) match {
      case address if AddressFamilies.contains(address) => address
      case otherAddressFamily =>
        throw HaProxyProtocolError(
          s"Decoding HaProxyMessage has failed. Incorrect address family, expected one of: " +
            s"[${AddressFamilies.mkString(", ")}], but instead got $otherAddressFamily"
        )
    }
    // take 4 lowest bits
    val transportProtocol = (addressFamilyAndTransportProtocol & 15).toByte match {
      case protocol if HaProxyTransportProtocol.byteValuesSet.contains(protocol) =>
        HaProxyTransportProtocol.byValues(protocol)
      case otherProtocol =>
        throw HaProxyProtocolError(
          s"Decoding HaProxyMessage has failed. Incorrect transport protocol, expected one of: " +
            s"[${HaProxyTransportProtocol.byteValuesSet.mkString(", ")}], but instead got $otherProtocol"
        )
    }

    val _ = bi.getUnsignedShort // length parameter
    val addresses: HaProxyAddresses =
      if (addressFamily == AddressFamilyIpV4) {
        val srcAddress = bi.getBytes(4)
        val dstAddress = bi.getBytes(4)
        val srcPort = bi.getUnsignedShort
        val dstPort = bi.getUnsignedShort
        HaProxyAddresses.HaProxyIpv4Addresses(
          src = HaProxyAddresses.HaProxyIpv4Address(InetAddress.getByAddress(srcAddress), srcPort),
          dst = HaProxyAddresses.HaProxyIpv4Address(InetAddress.getByAddress(dstAddress), dstPort)
        )
      }
      else if (addressFamily == AddressFamilyIpV6) {
        val srcAddress = bi.getBytes(HaProxyAddresses.Ipv6AddressLength)
        val dstAddress = bi.getBytes(HaProxyAddresses.Ipv6AddressLength)
        val srcPort = bi.getUnsignedShort
        val dstPort = bi.getUnsignedShort
        HaProxyAddresses.HaProxyIpv6Addresses(
          src = HaProxyAddresses.HaProxyIpv6Address(InetAddress.getByAddress(srcAddress), srcPort),
          dst = HaProxyAddresses.HaProxyIpv6Address(InetAddress.getByAddress(dstAddress), dstPort)
        )
      }
      else if (addressFamily == AddressFamilyUnix) {
        val srcAddress = ArraySeq.unsafeWrapArray(bi.getBytes(HaProxyAddresses.UnixAddressLength))
        val dstAddress = ArraySeq.unsafeWrapArray(bi.getBytes(HaProxyAddresses.UnixAddressLength))
        HaProxyAddresses.HaProxyUnixAddresses(
          src = HaProxyAddresses.HaProxyUnixAddress(srcAddress),
          dst = HaProxyAddresses.HaProxyUnixAddress(dstAddress)
        )
      }
      else {
        HaProxyAddresses.HaProxyUnspecifiedAddresses
      }

    HaProxyMessage(
      transportProtocol = transportProtocol,
      proxyAddresses = addresses
    )
  }

  private[proxy] def startsWithProxyProtocolSignature(bi: ByteIterator): Boolean = {
    var i = 0
    var isSignatureCorrect = true
    while (isSignatureCorrect && i < V2ProtocolSignature.length) {
      val b = bi.getByte
      isSignatureCorrect = b == V2ProtocolSignature(i)
      i += 1
    }
    i == V2ProtocolSignature.length && isSignatureCorrect
  }

  private def base64Message(fullMessage: ByteString): String = {
    new String(Base64.getEncoder.encode(fullMessage.toArrayUnsafe()))
  }

  private def expectV2Protocol(bi: ByteIterator, fullMessage: ByteString): Unit = {
    if (!startsWithProxyProtocolSignature(bi)) {
      throw HaProxyProtocolError(
        s"Decoding protocol signature has failed. Base64 encoded message: ${base64Message(fullMessage)}"
      )
    }
  }
}

sealed trait HaProxyDecodingFlowException extends Product with Serializable

object HaProxyDecodingFlowException {
  final case class DownstreamFailedBeforeHaProxyMessageMaterialized(downstreamFailure: Throwable)
      extends RuntimeException("Downstream has finished before HaProxyMessage could be materialized", downstreamFailure)
      with HaProxyDecodingFlowException

  final case class UpstreamFinishedBeforeHaProxyMessageMaterialized()
      extends RuntimeException("Upstream has finished before HaProxyMessage could be materialized")
      with HaProxyDecodingFlowException

  final case class UpstreamFailedBeforeHaProxyMessageMaterialized(upstreamFailure: Throwable)
      extends RuntimeException("Upstream has failed before HaProxyMessage could be materialized", upstreamFailure)
      with HaProxyDecodingFlowException
}

final class HaProxyDecodingFlow
    extends GraphStageWithMaterializedValue[FlowShape[ByteString, ByteString], Future[Option[HaProxyMessage]]] {
  import HaProxyProtocol._

  val in: Inlet[ByteString] = Inlet("HaProxyDecodingFlow.in")
  val out: Outlet[ByteString] = Outlet("HaProxyDecodingFlow.out")

  override val shape = FlowShape(in, out)

  override def createLogicAndMaterializedValue(inheritedAttributes: Attributes)
    : (GraphStageLogic, Future[Option[HaProxyMessage]]) = {
    val proxyMessagePromise = Promise[Option[HaProxyMessage]]()
    val logic: GraphStageLogic = new GraphStageLogic(shape) {

      val handler = new InHandler with OutHandler {
        private var alreadyMaterialized = false
        private var byteString = ByteString.empty

        override def onPush(): Unit = {
          val newByteString = grab(in)
          if (alreadyMaterialized) push(out, newByteString)
          else {
            byteString ++= newByteString
            // if signature doesn't match or is too short (signature should always be sent in a single packet), then fallback to "regular" traffic
            if (!HaProxyDecoder.startsWithProxyProtocolSignature(byteString.iterator)) {
              val _ = proxyMessagePromise.success(None)
              alreadyMaterialized = true
              push(out, newByteString)
            }
            else {
              if (byteString.length < HaProxyProtocol.HeaderLength) pull(in)
              else {
                val totalMessageLength = {
                  val bytesBesidesHeader = byteString.slice(14, 16).iterator.getUnsignedShort
                  HaProxyProtocol.HeaderLength + bytesBesidesHeader
                }
                if (byteString.length < totalMessageLength) pull(in)
                else {
                  val messageByteString = byteString.take(totalMessageLength)
                  try {
                    val decoded = HaProxyDecoder.decode(messageByteString)
                    val _ = proxyMessagePromise.success(Some(decoded))
                    alreadyMaterialized = true
                    if (byteString.length <= totalMessageLength) pull(in)
                    else push(out, byteString.drop(totalMessageLength))
                  }
                  catch {
                    case error: HaProxyProtocolError =>
                      val _ = proxyMessagePromise.failure(error)
                      alreadyMaterialized = true
                      failStage(error)
                  }
                }
              }
            }
          }
        }

        override def onPull(): Unit = {
          pull(in)
        }

        override def onDownstreamFinish(cause: Throwable): Unit = {
          if (!alreadyMaterialized) {
            val _ = proxyMessagePromise.failure(
              HaProxyDecodingFlowException.DownstreamFailedBeforeHaProxyMessageMaterialized(cause)
            )
          }
          cancelStage(cause)
        }

        override def onUpstreamFinish(): Unit = {
          if (!alreadyMaterialized) {
            val _ = proxyMessagePromise.failure(
              HaProxyDecodingFlowException.UpstreamFinishedBeforeHaProxyMessageMaterialized()
            )
          }
          completeStage()
        }

        override def onUpstreamFailure(ex: Throwable): Unit = {
          if (!alreadyMaterialized) {
            val _ = proxyMessagePromise.failure(
              HaProxyDecodingFlowException.UpstreamFailedBeforeHaProxyMessageMaterialized(ex)
            )
          }
          failStage(ex)
        }

      }
      setHandlers(in, out, handler)
    }
    (logic, proxyMessagePromise.future)
  }
}
 */
