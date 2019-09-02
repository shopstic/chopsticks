package dev.chopsticks.kvdb.util

import scala.concurrent.duration._

final case class KvdbClientOptions(maxBatchBytes: Int, tailPollingInterval: FiniteDuration)
object KvdbClientOptions {
  object Implicits {
    implicit val defaultClientOptions: KvdbClientOptions =
      KvdbClientOptions(maxBatchBytes = 32 * 1024, tailPollingInterval = 100.millis)
  }
}
