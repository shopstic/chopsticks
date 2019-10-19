package dev.chopsticks.kvdb.util

import squants.information.Information

import scala.concurrent.duration._
import squants.information.InformationConversions._

final case class KvdbClientOptions(maxBatchBytes: Information, tailPollingInterval: FiniteDuration = 100.millis, serdesParallelism: Int = 1)
object KvdbClientOptions {
  object Implicits {
    implicit val defaultClientOptions: KvdbClientOptions = KvdbClientOptions(32.kib)
  }
}
