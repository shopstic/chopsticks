package dev.chopsticks.fp

import dev.chopsticks.fp.config.HoconConfig
import dev.chopsticks.fp.util.PlatformUtils
import zio.{EnvironmentTag, ZIOApp, ZIOAppArgs, ZLayer}

trait ZApp extends ZIOApp:

  override type Environment = HoconConfig

  override val environmentTag: EnvironmentTag[Environment] = EnvironmentTag[HoconConfig]

  override val bootstrap: ZLayer[ZIOAppArgs, Any, Environment] =
    val executorLayer = zio.Runtime.setExecutor(PlatformUtils.createExecutor(
      corePoolSize = 0,
      maxPoolSize = Runtime.getRuntime.availableProcessors(),
      keepAliveTimeMs = 5000,
      threadPoolName = "zio-app-bootstrap"
    ))
    val hoconConfigLayer = HoconConfig.live(Some(this.getClass))
    executorLayer >+> hoconConfigLayer

end ZApp
