package dev.chopsticks.fdb.zio

import dev.chopsticks.kvdb.ColumnFamily
import dev.chopsticks.kvdb.codec.KeyConstraints
import dev.chopsticks.kvdb.codec.KeyConstraints.ConstraintsBuilder
import dev.chopsticks.kvdb.fdb.FdbReadApi
import eu.timepit.refined.types.numeric.PosInt
import zio.Task

class ZFdbKeyspaceReadApi[BCF[A, B] <: ColumnFamily[A, B], CF <: BCF[K, V], K, V](
  keyspace: CF,
  api: FdbReadApi[BCF]
) {
  def get(
    constraints: ConstraintsBuilder[K]
  ): Task[Option[(K, V)]] = {
    Task
      .fromCompletionStage(api.get(keyspace, KeyConstraints.build(constraints)(keyspace.keySerdes).constraints))
      .flatMap(maybePair => Task(maybePair.map(p => keyspace.unsafeDeserialize(p))))
  }

  def getValue(
    constraints: ConstraintsBuilder[K]
  ): Task[Option[V]] = {
    Task
      .fromCompletionStage(api.get(keyspace, KeyConstraints.build(constraints)(keyspace.keySerdes).constraints))
      .flatMap(maybePair => Task(maybePair.map(p => keyspace.unsafeDeserializeValue(p._2))))
  }

  def getRange(
    from: ConstraintsBuilder[K],
    to: ConstraintsBuilder[K],
    limit: PosInt
  ): Task[List[(K, V)]] = {
    Task
      .fromCompletionStage(api.getRange(keyspace, KeyConstraints.range(from, to, limit.value)(keyspace.keySerdes)))
      .flatMap(pairs => Task(pairs.map(p => keyspace.unsafeDeserialize(p))))
  }
}
