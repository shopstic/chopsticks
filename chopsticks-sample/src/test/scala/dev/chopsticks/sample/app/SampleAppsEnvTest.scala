package dev.chopsticks.sample.app

import dev.chopsticks.sample.app.dstream.{DstreamLoadTestMasterApp, DstreamLoadTestWorkerApp, DstreamSampleApp}
import dev.chopsticks.testkit.{LiveAppEnvTest, ManagedSystemProperties}
import zio.console.Console
import zio.test.TestAspect.sequential
import zio.test._

object SampleAppsEnvTest extends DefaultRunnableSpec with LiveAppEnvTest with ManagedSystemProperties {

  override def spec: ZSpec[Console, Throwable] =
    suite("Sample apps env test")(
      testM("bootstrap DstreamSampleApp") {
        bootstrapTest {
          DstreamSampleApp.create
        }
      },
      testM("bootstrap DstreamsSampleMasterApp") {
        bootstrapTest {
          DstreamLoadTestMasterApp.create
        }
      },
      testM("bootstrap DstreamsSampleWorkerApp") {
        bootstrapTest {
          DstreamLoadTestWorkerApp.create
        }
      }
    ) @@ sequential

}
