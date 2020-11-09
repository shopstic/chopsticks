package dev.chopsticks.sample.app

import dev.chopsticks.sample.app.dstreams.{DstreamSampleApp, DstreamsSampleMasterApp, DstreamsSampleWorkerApp}
import dev.chopsticks.testkit.{LiveAppEnvTest, ManagedProps}
import zio.console.Console
import zio.test.TestAspect.sequential
import zio.test._

object SampleAppsEnvTest extends DefaultRunnableSpec with LiveAppEnvTest with ManagedProps {

  override def spec: ZSpec[Console, Throwable] =
    suite("Sample apps env test")(
      testM("bootstrap DstreamSampleApp") {
        bootstrapTest {
          DstreamSampleApp.create
        }
      },
      testM("bootstrap DstreamsSampleMasterApp") {
        bootstrapTest {
          DstreamsSampleMasterApp.create
        }
      },
      testM("bootstrap DstreamsSampleWorkerApp") {
        bootstrapTest {
          DstreamsSampleWorkerApp.create
        }
      }
    ) @@ sequential

}
