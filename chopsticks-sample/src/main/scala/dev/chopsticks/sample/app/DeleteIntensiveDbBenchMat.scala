package dev.chopsticks.sample.app

import java.util.UUID

import dev.chopsticks.kvdb.codec.ValueSerdes
import dev.chopsticks.kvdb.codec.berkeleydb_key._
import dev.chopsticks.kvdb.rocksdb.RocksdbColumnFamilyConfig.{PointLookupPattern, TotalOrderScanPattern}
import dev.chopsticks.kvdb.rocksdb.{RocksdbColumnFamilyConfig, RocksdbColumnFamilyOptionsMap, RocksdbMaterialization}
import dev.chopsticks.kvdb.util.KvdbSerdesUtils
import dev.chopsticks.sample.app.DeleteIntensiveDbBenchApp.{DbDef, MtMessageId}
import squants.information.InformationConversions._

object DeleteIntensiveDbBenchMat extends DbDef.Materialization with RocksdbMaterialization[DbDef.BaseCf, DbDef.CfSet] {
  import DbDef._

  import scala.language.higherKinds
  implicit lazy val dbValue: ValueSerdes[MtMessageId] = ValueSerdes
    .create[MtMessageId](
      id => KvdbSerdesUtils.stringToByteArray(id.uuid.toString),
      bytes => Right(MtMessageId(UUID.fromString(KvdbSerdesUtils.byteArrayToString(bytes))))
    )

  object queue extends Queue
  object fact extends Fact
  object inflight extends Inflight

  val defaultColumnFamily: BaseCf[_, _] = queue

  val columnFamilyConfigMap: RocksdbColumnFamilyOptionsMap[BaseCf, CfSet] =
    RocksdbColumnFamilyOptionsMap[BaseCf]
      .of(
        queue,
        RocksdbColumnFamilyConfig(
          memoryBudget = 128.mib,
          blockCache = 1.gib,
          blockSize = 64.kib
        ).toOptions(TotalOrderScanPattern)
      )
      .and(
        fact,
        RocksdbColumnFamilyConfig(
          memoryBudget = 1.gib,
          blockCache = 1.gib,
          blockSize = 64.kib
        ).toOptions(PointLookupPattern)
      )
      .and(
        inflight,
        RocksdbColumnFamilyConfig(
          memoryBudget = 128.mib,
          blockCache = 1.gib,
          blockSize = 64.kib
        ).toOptions(PointLookupPattern)
      )
}
