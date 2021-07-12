package dev.chopsticks.kvdb.fdb

import com.apple.foundationdb.tuple.Versionstamp
import dev.chopsticks.kvdb.ColumnFamily
import dev.chopsticks.kvdb.codec.KeySerdes
import shapeless._
import shapeless.ops.hlist._

import scala.annotation.nowarn

trait FdbMaterialization[BCF[A, B] <: ColumnFamily[A, B]] {
  def keyspacesWithVersionstampKey: Set[FdbMaterialization.KeyspaceWithVersionstampKey[BCF]]
  def keyspacesWithVersionstampValue: Set[FdbMaterialization.KeyspaceWithVersionstampValue[BCF]]
}

object FdbMaterialization {
  trait KeyspaceWithVersionstampKey[BCF[A, B] <: ColumnFamily[A, B]] {
    def keyspace: BCF[_, _]
  }

  trait KeyspaceWithVersionstampValue[BCF[A, B] <: ColumnFamily[A, B]] {
    def keyspace: BCF[_, _]
  }

  object KeyspaceWithVersionstampKey {
    def apply[BCF[A, B] <: ColumnFamily[A, B], CF[A, B] <: ColumnFamily[A, B], C <: BCF[K, _], K, L <: HList](
      ks: CF[K, _] with C
    )(implicit
      @nowarn keySerdes: KeySerdes.Aux[K, L],
      @nowarn selector: Selector[L, Versionstamp]
    ): KeyspaceWithVersionstampKey[BCF] = {
      new KeyspaceWithVersionstampKey[BCF] {
        val keyspace: BCF[_, _] = ks
      }
    }
  }

  object KeyspaceWithVersionstampValue {
    def literal[BCF[A, B] <: ColumnFamily[A, B], CF[A, B] <: ColumnFamily[A, B], C <: BCF[_, Versionstamp]](
      ks: CF[_, Versionstamp] with C
    ): KeyspaceWithVersionstampValue[BCF] = {
      new KeyspaceWithVersionstampValue[BCF] {
        val keyspace: BCF[_, _] = ks
      }
    }

    def apply[BCF[A, B] <: ColumnFamily[A, B], CF[A, B] <: ColumnFamily[A, B], C <: BCF[_, V], V, L <: HList](
      ks: CF[_, V] with C
    )(implicit
      @nowarn keySerdes: KeySerdes.Aux[V, L],
      @nowarn selector: Selector[L, Versionstamp]
    ): KeyspaceWithVersionstampValue[BCF] = {
      new KeyspaceWithVersionstampValue[BCF] {
        val keyspace: BCF[_, _] = ks
      }
    }
  }
}
