package dev.chopsticks.kvdb.lmdb

import dev.chopsticks.kvdb.api.{KvdbDatabaseApi, KvdbDatabaseApiTest}

final class LmdbDatabaseApiTest extends KvdbDatabaseApiTest {
  protected val dbMat = LmdbDatabaseTest.dbMaterialization
  protected val managedDb = LmdbDatabaseTest.managedDb.mapM(KvdbDatabaseApi(_))
}
