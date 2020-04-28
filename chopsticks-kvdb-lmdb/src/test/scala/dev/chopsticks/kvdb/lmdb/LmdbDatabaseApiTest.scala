package dev.chopsticks.kvdb.lmdb

import dev.chopsticks.kvdb.api.KvdbDatabaseApi.KvdbApiClientOptions
import dev.chopsticks.kvdb.api.{KvdbDatabaseApi, KvdbDatabaseApiTest}
import scala.concurrent.duration._

final class LmdbDatabaseApiTest extends KvdbDatabaseApiTest {
  protected val dbMat = LmdbDatabaseTest.dbMaterialization
  protected val managedDb = LmdbDatabaseTest.managedDb.mapM { db =>
    KvdbDatabaseApi(db, KvdbApiClientOptions(tailPollingMaxInterval = 10.millis))
  }
}
