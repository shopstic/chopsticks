package dev.chopsticks.dstream

import org.apache.pekko.grpc.internal.HardcodedServiceDiscovery
import dev.chopsticks.dstream.DstreamWorker.DstreamWorkerConfig
import dev.chopsticks.dstream.test.DstreamSpecEnv
import dev.chopsticks.dstream.util.DstreamUtils
import dev.chopsticks.fp.config.{HoconConfig, TypedConfig}
import logstage.Log
import zio.test.Assertion._
import zio.test._

//noinspection TypeAnnotation
object DstreamUtilsSpec extends ZIOSpecDefault {
  import dev.chopsticks.dstream.test.DstreamTestUtils.ToTestZLayer

  lazy val hoconConfigLayer = HoconConfig.live(Some(getClass)).forTest

  lazy val typedConfigLayer = DstreamUtils.liveWorkerTypedConfig(logLevel = Log.Level.Debug).forTest

  override def spec = suite("DstreamUtils")(
    test("liveWorkerTypedConfig") {
      for {
        workerConfig <- TypedConfig.get[DstreamWorkerConfig]
      } yield {
        val settings = workerConfig.clientSettings

        assert(settings.serviceDiscovery)(isSubtype[HardcodedServiceDiscovery](anything)) &&
        assert(settings.connectionAttempts)(equalTo(None))
      }
    }
      .provide(
        hoconConfigLayer,
        DstreamSpecEnv.izLoggingRouterLayer,
        DstreamSpecEnv.izLoggingLayer,
        DstreamSpecEnv.PekkoEnvLayer,
        typedConfigLayer
      )
  )
}
