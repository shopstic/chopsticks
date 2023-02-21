package dev.chopsticks.kvdb.fdb

import com.apple.foundationdb.tuple.Versionstamp
import dev.chopsticks.kvdb.ColumnFamily
import dev.chopsticks.kvdb.codec.KeySerdes

import scala.annotation.nowarn

trait FdbMaterialization:
  def keyspacesWithVersionstampKey: Set[FdbMaterialization.KeyspaceWithVersionstampKey[_]]
  def keyspacesWithVersionstampValue: Set[FdbMaterialization.KeyspaceWithVersionstampValue[_]]

object FdbMaterialization:
  type VersionstampSelector[T <: Tuple] <: Int =
    T match
      case EmptyTuple => Nothing
      case Versionstamp *: _ => 0
      case _ *: t => scala.compiletime.ops.int.S[VersionstampSelector[t]]

  trait KeyspaceWithVersionstampKey[CF <: ColumnFamily[_, _]]:
    def keyspace: CF

  trait KeyspaceWithVersionstampValue[CF <: ColumnFamily[_, _]]:
    def keyspace: CF

  object KeyspaceWithVersionstampKey:
    def apply[C <: ColumnFamily[_, _]](ks: C)(using
      @nowarn keySerdes: KeySerdes[ks.Key]
    )(using _ <:< VersionstampSelector[keySerdes.Flattened]): KeyspaceWithVersionstampKey[C] =
      new KeyspaceWithVersionstampKey[C]:
        val keyspace: C = ks
  end KeyspaceWithVersionstampKey

  object KeyspaceWithVersionstampValue:
    def literal[C <: ColumnFamily[_, Versionstamp]](ks: C): KeyspaceWithVersionstampValue[C] =
      new KeyspaceWithVersionstampValue[C]:
        val keyspace: C = ks

    def apply[C <: ColumnFamily[_, _]](ks: C)(using
      @nowarn valueSerdes: KeySerdes[ks.Value]
    )(using _ <:< VersionstampSelector[valueSerdes.Flattened]): KeyspaceWithVersionstampValue[C] =
      new KeyspaceWithVersionstampValue[C]:
        val keyspace: C = ks

  end KeyspaceWithVersionstampValue

end FdbMaterialization
