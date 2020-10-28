package dev.chopsticks.sample.kvdb

import java.time.Instant

import dev.chopsticks.kvdb.{ColumnFamilySet, KvdbDefinition, KvdbMaterialization}
import dev.chopsticks.kvdb.api.KvdbDatabaseApi
import dev.chopsticks.kvdb.codec.ValueSerdes
import dev.chopsticks.kvdb.fdb.FdbMaterialization
import dev.chopsticks.kvdb.fdb.FdbMaterialization.{KeyspaceWithVersionstampKey, KeyspaceWithVersionstampValue}
import dev.chopsticks.kvdb.rocksdb.RocksdbColumnFamilyConfig.PrefixedScanPattern
import dev.chopsticks.kvdb.rocksdb.{RocksdbColumnFamilyConfig, RocksdbColumnFamilyOptionsMap, RocksdbMaterialization}

object MultiBackendSampleDb {
  object Definition extends KvdbDefinition {
    final case class TestKey(name: String, time: Instant, version: Int)
    final case class TestValue(quantity: Long, amount: Double)

    trait Default extends BaseCf[TestKey, TestValue]

    type CfSet = Default

    trait DbStorage extends KvdbMaterialization[BaseCf, CfSet] {
      def default: Default
      override def columnFamilySet: ColumnFamilySet[BaseCf, CfSet] = ColumnFamilySet[BaseCf].of(default)
    }

    trait FdbStorage extends DbStorage with FdbMaterialization[BaseCf] {
      override def keyspacesWithVersionstampKey: Set[KeyspaceWithVersionstampKey[BaseCf]] = Set.empty
      override def keyspacesWithVersionstampValue: Set[KeyspaceWithVersionstampValue[BaseCf]] = Set.empty
    }

    trait RocksdbStorage extends DbStorage with RocksdbMaterialization[BaseCf, CfSet] {
      override def defaultColumnFamily: BaseCf[_, _] = default
      override def columnFamilyConfigMap: RocksdbColumnFamilyOptionsMap[BaseCf, CfSet] = {
        import squants.information.InformationConversions._
        import eu.timepit.refined.auto._

        RocksdbColumnFamilyOptionsMap[BaseCf]
          .of(
            default,
            RocksdbColumnFamilyConfig(
              memoryBudget = 1.mib,
              blockCache = 1.mib,
              blockSize = 8.kib,
              writeBufferCount = 4
            ).toOptions(PrefixedScanPattern(1))
          )
      }
    }

    sealed trait DbService {
      def api: KvdbDatabaseApi[BaseCf]
      def storage: DbStorage
    }

    final case class FdbService(api: KvdbDatabaseApi[BaseCf], storage: FdbStorage) extends DbService
    final case class RocksdbService(api: KvdbDatabaseApi[BaseCf], storage: RocksdbStorage) extends DbService
  }

  object Backends {
    import Definition._

    object fdbStorage extends FdbStorage {
      import dev.chopsticks.kvdb.codec.fdb_key._
      implicit val valueSerdes: ValueSerdes[TestValue] = ValueSerdes.fromKeySerdes

      object default extends Default
    }

    object rocksdbStorage extends RocksdbStorage {
      import dev.chopsticks.kvdb.codec.berkeleydb_key._
      implicit val valueSerdes: ValueSerdes[TestValue] = ValueSerdes.fromKeySerdes

      object default extends Default
    }
  }
}
