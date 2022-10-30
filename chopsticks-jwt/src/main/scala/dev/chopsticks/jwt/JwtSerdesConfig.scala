package dev.chopsticks.jwt

import pdi.jwt.algorithms.JwtAsymmetricAlgorithm
import pureconfig.ConfigReader

import java.security.spec.{PKCS8EncodedKeySpec, X509EncodedKeySpec}
import java.util.Base64
import scala.util.Try

final case class JwtSerdesConfig(
  privateKey: PKCS8EncodedKeySpec,
  publicKey: X509EncodedKeySpec,
  algorithm: JwtAsymmetricAlgorithm
) {
  def encoderConfig: JwtEncoderConfig = JwtEncoderConfig(privateKey, algorithm)
  def decoderConfig: JwtDecoderConfig = JwtDecoderConfig(publicKey, algorithm)
}

object JwtSerdesConfig {
  implicit lazy val publicKeyReader: ConfigReader[X509EncodedKeySpec] =
    ConfigReader.fromNonEmptyStringTry { base64Encoded =>
      Try(new X509EncodedKeySpec(Base64.getDecoder.decode(base64Encoded)))
    }
  implicit lazy val privateKeyReader: ConfigReader[PKCS8EncodedKeySpec] =
    ConfigReader.fromNonEmptyStringTry { base64Encoded =>
      Try(new PKCS8EncodedKeySpec(Base64.getDecoder.decode(base64Encoded)))
    }
  // noinspection TypeAnnotation
  implicit lazy val configReader = {
    import JwtAsymmetricAlgorithm.pureconfigReader
    import dev.chopsticks.util.config.PureconfigConverters._
    ConfigReader[JwtSerdesConfig]
  }
}
