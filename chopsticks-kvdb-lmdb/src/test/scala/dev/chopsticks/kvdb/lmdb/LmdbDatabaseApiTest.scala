package dev.chopsticks.kvdb.lmdb

import dev.chopsticks.kvdb.api.{KvdbDatabaseApi, KvdbDatabaseApiTest}
import scala.concurrent.duration._

final class LmdbDatabaseApiTest extends KvdbDatabaseApiTest {
  protected val dbMat = LmdbDatabaseTest.dbMaterialization
  protected val managedDb = LmdbDatabaseTest.managedDb
    .flatMap(KvdbDatabaseApi(_))
    .map(_.withOptions(_.copy(tailPollingMaxInterval = 10.millis)))
}
