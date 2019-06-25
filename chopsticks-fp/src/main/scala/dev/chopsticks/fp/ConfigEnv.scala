package dev.chopsticks.fp

trait ConfigEnv[Cfg] {
  def config: Cfg
}
