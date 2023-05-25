package dev.chopsticks.jwt

import pdi.jwt.algorithms.{JwtAsymmetricAlgorithm, JwtECDSAAlgorithm, JwtEdDSAAlgorithm, JwtRSAAlgorithm}
import pureconfig.{ConfigReader, ConfigWriter}
import pureconfig.generic.semiauto.deriveEnumerationReader

import java.security.KeyFactory

object JwtAsymmetricAlgorithm {
  implicit lazy val configReader: ConfigReader[JwtAsymmetricAlgorithm] = {
    val jwtRSAAlgorithmReader = deriveEnumerationReader[JwtRSAAlgorithm]((name: String) => name)
    val jwtECDSAAlgorithmReader = deriveEnumerationReader[JwtECDSAAlgorithm]((name: String) => name)
    val jwtEdDSAAlgorithmReader = deriveEnumerationReader[JwtEdDSAAlgorithm]((name: String) => name)

    ConfigReader.fromFunction(value => {
      jwtRSAAlgorithmReader.from(value)
        .orElse(jwtECDSAAlgorithmReader.from(value))
        .orElse(jwtEdDSAAlgorithmReader.from(value))
    })
  }

  implicit lazy val configWriter: ConfigWriter[JwtAsymmetricAlgorithm] = ConfigWriter.toString(_.name)

  def createKeyFactory(algorithm: JwtAsymmetricAlgorithm): KeyFactory = {
    KeyFactory.getInstance(algorithm match {
      case _: JwtRSAAlgorithm => "RSA"
      case _: JwtECDSAAlgorithm => "ECDSA"
      case _: JwtEdDSAAlgorithm => "Ed25519"
    })
  }
}
