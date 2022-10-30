package dev.chopsticks.jwt

import io.circe.Decoder
import pdi.jwt.JwtCirce
import pdi.jwt.algorithms.JwtAsymmetricAlgorithm
import pureconfig.ConfigReader
import zio.{Tag, Task, UIO, URIO, URManaged, ZIO, ZLayer, ZManaged}

import java.security.spec.X509EncodedKeySpec
import scala.util.Try

final case class JwtDecoderConfig(
  publicKey: X509EncodedKeySpec,
  algorithm: JwtAsymmetricAlgorithm
)

object JwtDecoderConfig {
  // noinspection TypeAnnotation
  implicit lazy val configReader = {
    import JwtAsymmetricAlgorithm.pureconfigReader
    import JwtSerdesConfig.publicKeyReader
    import dev.chopsticks.util.config.PureconfigConverters._
    ConfigReader[JwtDecoderConfig]
  }
}

object JwtDecoder {
  def getManaged[C: Tag]: URManaged[JwtDecoder[C], Service[C]] = ZManaged.access[JwtDecoder[C]](_.get)
  def get[C: Tag]: URIO[JwtDecoder[C], Service[C]] = ZIO.access[JwtDecoder[C]](_.get)

  trait Service[C] {
    def decode(token: String): Try[JwtClaim[C]]
  }

  def live[C: Decoder: Tag](config: JwtDecoderConfig): ZLayer[Any, Throwable, JwtDecoder[C]] = {
    val effect = for {
      keyFactory <- UIO { JwtAsymmetricAlgorithm.createKeyFactory(config.algorithm) }
      publicKey <- Task(keyFactory.generatePublic(config.publicKey))
    } yield {
      new Service[C] {
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

    effect.toLayer
  }
}
