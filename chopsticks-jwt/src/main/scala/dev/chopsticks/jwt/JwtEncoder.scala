package dev.chopsticks.jwt

import io.circe.Encoder
import pdi.jwt.JwtCirce
import pdi.jwt.algorithms.JwtAsymmetricAlgorithm
import pureconfig.ConfigConvert
import zio.{Tag, Task, UIO, URIO, URManaged, ZIO, ZLayer, ZManaged}

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

object JwtEncoder {
  def getManaged[C: Tag]: URManaged[JwtEncoder[C], Service[C]] = ZManaged.access[JwtEncoder[C]](_.get)
  def get[C: Tag]: URIO[JwtEncoder[C], Service[C]] = ZIO.access[JwtEncoder[C]](_.get)

  trait Service[C] {
    def encode(claim: JwtClaim[C]): String
  }

  def live[C: Encoder: Tag](config: JwtEncoderConfig): ZLayer[Any, Throwable, JwtEncoder[C]] = {
    val effect = for {
      keyFactory <- UIO { JwtAsymmetricAlgorithm.createKeyFactory(config.algorithm) }
      privateKey <- Task(keyFactory.generatePrivate(config.privateKey))
    } yield {
      val encoder = implicitly[Encoder[C]]

      new Service[C] {
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

    effect.toLayer
  }
}
