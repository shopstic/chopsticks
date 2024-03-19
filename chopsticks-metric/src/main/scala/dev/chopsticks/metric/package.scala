package dev.chopsticks

import dev.chopsticks.fp.util.SharedResourceManager

package object metric {
  type MetricServiceManager[Cfg, Svc] = SharedResourceManager[Any, Cfg, Svc]
}
