package dev.chopsticks.sample.util

import org.apache.pekko.actor.{OneForOneStrategy, SupervisorStrategy, SupervisorStrategyConfigurator}

final class JustCrashSupervisorStrategy extends SupervisorStrategyConfigurator {
  override def create(): SupervisorStrategy =
    OneForOneStrategy() {
      case _ =>
        SupervisorStrategy.Escalate // Just terminate the whole system
    }
}
