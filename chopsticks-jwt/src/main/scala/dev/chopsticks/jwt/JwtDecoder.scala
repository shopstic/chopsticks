package dev.chopsticks.jwt

import io.circe.Decoder
import pdi.jwt.JwtCirce
import pdi.jwt.algorithms.JwtAsymmetricAlgorithm
import pureconfig.ConfigConvert
import zio.{Tag, URIO, ZIO, ZLayer}

import java.security.spec.X509EncodedKeySpec
import scala.util.Try

final case class JwtDecoderConfig(
  publicKey: X509EncodedKeySpec,
  algorithm: JwtAsymmetricAlgorithm
)

object JwtDecoderConfig {
  // noinspection TypeAnnotation
  implicit lazy val configConvert = {
    import JwtAsymmetricAlgorithm._
    import JwtSerdesConfig._
    import dev.chopsticks.util.config.PureconfigConverters._
    ConfigConvert[JwtDecoderConfig]
  }
}

trait JwtDecoder[C] {
  def decode(token: String): Try[JwtClaim[C]]
}

object JwtDecoder {
  def get[C: Tag]: URIO[JwtDecoder[C], JwtDecoder[C]] = ZIO.service[JwtDecoder[C]]

  def live[C: Decoder: Tag](config: JwtDecoderConfig): ZLayer[Any, Throwable, JwtDecoder[C]] = {
    val effect = for {
      keyFactory <- ZIO.succeed { JwtAsymmetricAlgorithm.createKeyFactory(config.algorithm) }
      publicKey <- ZIO.attempt(keyFactory.generatePublic(config.publicKey))
    } yield {
      new JwtDecoder[C] {
        override def decode(token: String): Try[JwtClaim[C]] = {
          JwtCirce
            .decode(token, publicKey, Seq(config.algorithm))
            .flatMap { claim =>
              io.circe.parser.decode[C](claim.content)
                .map { content =>
                  JwtClaim[C](
                    content = content,
                    issuer = claim.issuer,
                    subject = claim.subject,
                    audience = claim.audience,
                    expiration = claim.expiration,
                    notBefore = claim.notBefore,
                    issuedAt = claim.issuedAt,
                    jwtId = claim.jwtId
                  )
                }
                .toTry
            }
        }
      }
    }

    ZLayer.scoped(effect)
  }
}
