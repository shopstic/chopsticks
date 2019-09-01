package dev.chopsticks.kvdb.lmdb

import dev.chopsticks.fp.AkkaEnv
import dev.chopsticks.kvdb.api.{KvdbDatabaseApi, KvdbDatabaseApiTest}
import zio.ZManaged

final class LmdbDatabaseApiTest extends KvdbDatabaseApiTest {
  protected val defaultCf = LmdbDatabaseTest.defaultCf
  protected val lookupCf = LmdbDatabaseTest.lookupCf

  protected val managedDb = {
    for {
      db <- LmdbDatabaseTest.managedDb
      dbApi <- ZManaged.environment[AkkaEnv].map { implicit env =>
        KvdbDatabaseApi(db)
      }
    } yield dbApi
  }
}
