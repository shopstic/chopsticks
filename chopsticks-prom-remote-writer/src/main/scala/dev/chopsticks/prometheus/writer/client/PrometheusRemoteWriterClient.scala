package dev.chopsticks.prometheus.writer.client

import dev.chopsticks.prometheus.writer.proto.WriteRequest
import pureconfig.ConfigConvert
import sttp.client3.SttpBackend
import sttp.model.Uri
import zio.{Task, URIO, ZIO, ZManaged}

import scala.concurrent.Future
import scala.util.Try

final case class PrometheusRemoteWriterClientConfig(uri: Uri)

object PrometheusRemoteWriterClientConfig {
  implicit val configCovert = {
    import dev.chopsticks.util.config.PureconfigConverters._
    implicit val uriConfigConvert = ConfigConvert.viaStringTry[Uri](
      rawUri => Try(Uri.unsafeParse(rawUri)),
      uri => uri.toString()
    )
    ConfigConvert[PrometheusRemoteWriterClientConfig]
  }
}

object PrometheusRemoteWriterClient {
  def get: URIO[PrometheusRemoteWriterClient, Service] = ZIO.service[Service]
  def getManaged: ZManaged[PrometheusRemoteWriterClient, Nothing, Service] = ZManaged.service[Service]

  trait Service {
    def write(writeRequest: WriteRequest): Task[Unit]
  }
}

final case class LivePrometheusRemoteWriterClient(
  backend: SttpBackend[Future, _],
  config: PrometheusRemoteWriterClientConfig
) extends PrometheusRemoteWriterClient.Service {
  import sttp.client3._

  private val baseRequest = basicRequest
    .response(asString.getRight)
    .post(config.uri)
    .headers(
      Map(
        "Content-Encoding" -> "snappy",
        "X-Prometheus-Remote-Write-Version" -> "0.1.0"
      )
    )

  override def write(writeRequest: WriteRequest): Task[Unit] = {
    val bytes = org.xerial.snappy.Snappy.compress(writeRequest.toByteArray)
    Task
      .fromFuture { _ =>
        baseRequest.body(bytes).contentType("application/x-protobuf").send(backend)
      }
      .unit
  }
}
