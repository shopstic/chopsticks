package dev.chopsticks.fdb.transaction

import dev.chopsticks.kvdb.ColumnFamily
import dev.chopsticks.kvdb.codec.KeyConstraints
import dev.chopsticks.kvdb.codec.KeyConstraints.ConstraintsBuilder
import dev.chopsticks.kvdb.fdb.FdbReadApi
import dev.chopsticks.kvdb.util.KvdbAliases.KvdbPair
import eu.timepit.refined.types.numeric.PosInt
import zio.{Task, ZIO}

class ZFdbKeyspaceReadApi[CFS <: ColumnFamily[_, _], CF <: ColumnFamily[_, _]](
  val keyspace: CF,
  val api: FdbReadApi[CFS]
)(using CFS <:< CF):
  def getRaw(
    constraints: ConstraintsBuilder[keyspace.Key]
  ): Task[Option[KvdbPair]] =
    ZIO
      .fromCompletionStage(api.get(keyspace, KeyConstraints.build(constraints)(keyspace.keySerdes).constraints))

  def get(
    constraints: ConstraintsBuilder[keyspace.Key]
  ): Task[Option[(keyspace.Key, keyspace.Value)]] =
    getRaw(constraints)
      .flatMap(maybePair => ZIO.attempt(maybePair.map(p => keyspace.unsafeDeserialize(p))))

  def getValue(
    constraints: ConstraintsBuilder[keyspace.Key]
  ): Task[Option[keyspace.Value]] =
    ZIO
      .fromCompletionStage(api.get(keyspace, KeyConstraints.build(constraints)(keyspace.keySerdes).constraints))
      .flatMap(maybePair => ZIO.attempt(maybePair.map(p => keyspace.unsafeDeserializeValue(p._2))))

  def getRangeRaw(
    from: ConstraintsBuilder[keyspace.Key],
    to: ConstraintsBuilder[keyspace.Key],
    limit: PosInt,
    reverse: Boolean
  ): Task[List[KvdbPair]] =
    ZIO
      .fromCompletionStage(api.getRange(
        keyspace,
        KeyConstraints.range(from, to, limit.value)(keyspace.keySerdes),
        reverse
      ))

  def getRange(
    from: ConstraintsBuilder[keyspace.Key],
    to: ConstraintsBuilder[keyspace.Key],
    limit: PosInt,
    reverse: Boolean
  ): Task[List[(keyspace.Key, keyspace.Value)]] =
    getRangeRaw(from, to, limit, reverse)
      .flatMap(pairs => ZIO.attempt(pairs.map(p => keyspace.unsafeDeserialize(p))))

end ZFdbKeyspaceReadApi
