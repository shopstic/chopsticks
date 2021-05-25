package dev.chopsticks.sample.app

import dev.chopsticks.fp.ZAkkaApp
import dev.chopsticks.fp.ZAkkaApp.ZAkkaAppEnv
import dev.chopsticks.fp.config.TypedConfig
import dev.chopsticks.kvdb.fdb.FdbDatabase
import dev.chopsticks.kvdb.util.KvdbIoThreadPool
import zio._

object FdbWatchTestNewApp extends ZAkkaApp {
  override def run(args: List[String]): RIO[ZAkkaAppEnv, ExitCode] = {
    val app = FdbWatchTestApp.app

    import zio.magic._

    val dbLayer = ZManaged
      .access[TypedConfig[FdbWatchTestAppConfig]](_.get.config)
      .flatMap { appConfig =>
        FdbDatabase.manage(FdbWatchTestApp.sampleDb, appConfig.db)
      }
      .toLayer

    app
      .injectSome[ZAkkaAppEnv](
        TypedConfig.live[FdbWatchTestAppConfig](),
        dbLayer,
        KvdbIoThreadPool.live()
      )
      .as(ExitCode(0))
  }
}
