package dev.chopsticks.jwt

import pdi.jwt.algorithms.JwtAsymmetricAlgorithm
import pureconfig.ConfigConvert

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
  implicit lazy val publicKeyConvert: ConfigConvert[X509EncodedKeySpec] = ConfigConvert.viaNonEmptyStringTry(
    base64Encoded => Try(new X509EncodedKeySpec(Base64.getDecoder.decode(base64Encoded))),
    spec => Base64.getEncoder.encodeToString(spec.getEncoded)
  )

  implicit lazy val privateKeyConvert: ConfigConvert[PKCS8EncodedKeySpec] = ConfigConvert.viaNonEmptyStringTry(
    base64Encoded => Try(new PKCS8EncodedKeySpec(Base64.getDecoder.decode(base64Encoded))),
    spec => Base64.getEncoder.encodeToString(spec.getEncoded)
  )

  // noinspection TypeAnnotation
  implicit lazy val configConvert = {
    import JwtAsymmetricAlgorithm._
    import dev.chopsticks.util.config.PureconfigConverters._
    ConfigConvert[JwtSerdesConfig]
  }
}
