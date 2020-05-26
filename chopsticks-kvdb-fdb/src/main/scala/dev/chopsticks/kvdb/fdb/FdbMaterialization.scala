package dev.chopsticks.kvdb.fdb

import com.apple.foundationdb.tuple.Versionstamp
import dev.chopsticks.kvdb.ColumnFamily
import dev.chopsticks.kvdb.codec.KeySerdes
import shapeless._
import ops.hlist._

import scala.annotation.nowarn

trait FdbMaterialization[BCF[A, B] <: ColumnFamily[A, B]] {
  def keyspacesWithVersionstamp: Set[FdbMaterialization.KeyspaceWithVersionstamp[BCF]]
}

object FdbMaterialization {
  trait KeyspaceWithVersionstamp[BCF[A, B] <: ColumnFamily[A, B]] {
    def keyspace: BCF[_, _]
  }

  object KeyspaceWithVersionstamp {
    def apply[BCF[A, B] <: ColumnFamily[A, B], CF[A, B] <: ColumnFamily[A, B], C <: BCF[K, _], K, L <: HList](
      ks: CF[K, _] with C
    )(implicit
      @nowarn keySerdes: KeySerdes.Aux[K, L],
      @nowarn selector: Selector[L, Versionstamp]
    ): KeyspaceWithVersionstamp[BCF] = {
      new KeyspaceWithVersionstamp[BCF] {
        val keyspace: BCF[_, _] = ks
      }
    }
  }
}
