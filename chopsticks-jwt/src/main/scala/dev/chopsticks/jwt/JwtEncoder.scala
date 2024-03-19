package dev.chopsticks.jwt

import io.circe.Encoder
import pdi.jwt.JwtCirce
import pdi.jwt.algorithms.JwtAsymmetricAlgorithm
import pureconfig.ConfigConvert
import zio.{Tag, URIO, ZIO, ZLayer}

import java.security.spec.PKCS8EncodedKeySpec

final case class JwtEncoderConfig(
  privateKey: PKCS8EncodedKeySpec,
  algorithm: JwtAsymmetricAlgorithm
)

object JwtEncoderConfig {
  // noinspection TypeAnnotation
  implicit lazy val configConvert = {
    import JwtAsymmetricAlgorithm._
    import JwtSerdesConfig._
    import dev.chopsticks.util.config.PureconfigConverters._
    ConfigConvert[JwtEncoderConfig]
  }
}

trait JwtEncoder[C] {
  def encode(claim: JwtClaim[C]): String
}

object JwtEncoder {
  def get[C: Tag]: URIO[JwtEncoder[C], JwtEncoder[C]] = ZIO.service[JwtEncoder[C]]

  def live[C: Encoder: Tag](config: JwtEncoderConfig): ZLayer[Any, Throwable, JwtEncoder[C]] = {
    val effect = for {
      keyFactory <- ZIO.succeed { JwtAsymmetricAlgorithm.createKeyFactory(config.algorithm) }
      privateKey <- ZIO.attempt(keyFactory.generatePrivate(config.privateKey))
    } yield {
      val encoder = implicitly[Encoder[C]]

      new JwtEncoder[C] {
        override def encode(claim: JwtClaim[C]): String = {
          JwtCirce.encode(
            pdi.jwt.JwtClaim(
              content = encoder(claim.content).noSpaces,
              issuer = claim.issuer,
              subject = claim.subject,
              audience = claim.audience,
              expiration = claim.expiration,
              notBefore = claim.notBefore,
              issuedAt = claim.issuedAt,
              jwtId = claim.jwtId
            ),
            privateKey,
            config.algorithm
          )
        }
      }
    }

    ZLayer.fromZIO(effect)
  }
}
