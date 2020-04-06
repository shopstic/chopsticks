package dev.chopsticks.kvdb.api

import java.time.Instant

import akka.NotUsed
import akka.stream.scaladsl.{Flow, Source}
import com.google.protobuf.ByteString
import dev.chopsticks.fp.akka_env.AkkaEnv
import dev.chopsticks.kvdb.codec.KeyConstraints.{ConstraintsBuilder, ConstraintsRangesBuilder, ConstraintsSeqBuilder}
import dev.chopsticks.kvdb.codec.{KeyConstraints, KeyTransformer}
import dev.chopsticks.kvdb.proto.KvdbKeyConstraint.Operator
import dev.chopsticks.kvdb.proto.{KvdbKeyConstraint, KvdbKeyConstraintList}
import dev.chopsticks.kvdb.util.KvdbAliases.{KvdbBatch, KvdbTailBatch, KvdbValueBatch}
import dev.chopsticks.kvdb.util.KvdbClientOptions
import dev.chopsticks.kvdb.{ColumnFamily, KvdbDatabase}
import dev.chopsticks.stream.AkkaStreamUtils
import dev.chopsticks.fp.zio_ext._
import zio.Task

import scala.collection.immutable.Queue
import scala.collection.mutable
import scala.concurrent.Future
import scala.concurrent.duration._

final class KvdbColumnFamilyApi[BCF[A, B] <: ColumnFamily[A, B], CF <: BCF[K, V], K, V] private[kvdb] (
  db: KvdbDatabase[BCF, _],
  cf: CF
)(
  implicit rt: zio.Runtime[AkkaEnv]
) {
  private val akkaEnv = rt.environment.get[AkkaEnv.Service]
  import akkaEnv._

  def estimateCountTask: Task[Long] = {
    db.estimateCount(cf)
  }

  def getTask(constraints: ConstraintsBuilder[K]): Task[Option[(K, V)]] = {
    db.getTask(cf, KeyConstraints.build(constraints))
      .map(_.map(cf.unsafeDeserialize))
  }

  def getKeyTask(constraints: ConstraintsBuilder[K]): Task[Option[K]] = {
    db.getTask(cf, KeyConstraints.build(constraints))
      .map(_.map(p => cf.unsafeDeserializeKey(p._1)))
  }

  def getValueTask(constraints: ConstraintsBuilder[K]): Task[Option[V]] = {
    db.getTask(cf, KeyConstraints.build(constraints))
      .map(_.map(p => cf.unsafeDeserializeValue(p._2)))
  }

  def batchGetTask(constraints: ConstraintsSeqBuilder[K]): Task[Seq[Option[(K, V)]]] = {
    db.batchGetTask(cf, constraints(KeyConstraints.seed[K]).map(KeyConstraints.toList[K]))
      .map(_.map(_.map(cf.unsafeDeserialize)))
  }

  def batchGetByKeysTask(keys: Seq[K]): Task[Seq[Option[(K, V)]]] = {
    db.batchGetTask(cf, keys.map(k => KeyConstraints.toList(KeyConstraints.seed[K].is(k)(cf.keySerdes))))
      .map(_.map(_.map(cf.unsafeDeserialize)))
  }

  def batchGetFlow[O](
    constraintsMapper: O => ConstraintsBuilder[K],
    maxBatchSize: Int = 8096,
    groupWithin: FiniteDuration = Duration.Zero,
    useCache: Boolean = false
  ): Flow[O, (O, Option[V]), NotUsed] = {
    val flow = if (groupWithin > Duration.Zero) {
      Flow[O]
        .groupedWithin(maxBatchSize, groupWithin)
        .map { keyBatch =>
          (keyBatch, keyBatch.map(k => KeyConstraints.toList(constraintsMapper(k)(KeyConstraints.seed[K]))))
        }
    }
    else {
      Flow[O]
        .batch(
          maxBatchSize.toLong, { o: O =>
            val keyBatch = Vector.newBuilder[O]
            val valueBatch = Vector.newBuilder[KvdbKeyConstraintList]
            (keyBatch += o, valueBatch += KeyConstraints.toList(constraintsMapper(o)(KeyConstraints.seed[K])))
          }
        ) {
          case ((keyBatch, valueBatch), o) =>
            (keyBatch += o, valueBatch += KeyConstraints.toList(constraintsMapper(o)(KeyConstraints.seed[K])))
        }
        .map {
          case (keyBatch, valueBatch) =>
            (keyBatch.result(), valueBatch.result())
        }
    }

    flow
      .via(AkkaStreamUtils.statefulMapOptionFlow(() => {
        val cache = mutable.Map.empty[KvdbKeyConstraintList, Option[V]]

        {
          case (keyBatch: Seq[O], requests: Seq[KvdbKeyConstraintList]) =>
            val future = if (useCache) {
              val maybeValues = new Array[Option[V]](requests.size)
              val uncachedIndicesBuilder = mutable.ArrayBuilder.make[Int]
              val uncachedGetsBuilder = mutable.ArrayBuilder.make[KvdbKeyConstraintList]

              for ((request, index) <- requests.zipWithIndex) {
                cache.get(request) match {
                  case Some(v) => maybeValues(index) = v
                  case None =>
                    //
                    {
                      val _ = uncachedIndicesBuilder += index
                    }
                    val _ = uncachedGetsBuilder += request
                }
              }

              val uncachedRequests = uncachedGetsBuilder.result()

              if (uncachedRequests.isEmpty) {
                Future.successful(keyBatch.zip(maybeValues))
              }
              else {
                val uncachedIndices = uncachedIndicesBuilder.result()

                db.batchGetTask(cf, uncachedRequests.toList)
                  .map { pairs =>
                    for ((maybePair, index) <- pairs.zipWithIndex) {
                      val maybeValue = maybePair.map(p => cf.unsafeDeserializeValue(p._2))
                      maybeValues(uncachedIndices(index)) = maybeValue
                      cache.update(uncachedRequests(index), maybeValue)
                    }

                    keyBatch.zip(maybeValues)
                  }
                  .unsafeRunToFuture
              }
            }
            else {
              db.batchGetTask(cf, requests)
                .map { maybePairs => keyBatch.zip(maybePairs.map(_.map(p => cf.unsafeDeserializeValue(p._2)))) }
                .unsafeRunToFuture
            }

            Some(future)
        }
      }))
      .mapAsync(1)(identity)
      .mapConcat(identity)
  }

  def batchGetByKeysFlow[In, Out](
    extractor: In => Out,
    maxBatchSize: Int = 8096,
    groupWithin: FiniteDuration = Duration.Zero,
    useCache: Boolean = false
  )(implicit transformer: KeyTransformer[Out, K]): Flow[In, (In, Option[V]), NotUsed] = {
    batchGetFlow(
      (in: In) =>
        _ => {
          val key = transformer.transform(extractor(in))
          KeyConstraints[K](Queue(KvdbKeyConstraint(Operator.EQUAL, ByteString.copyFrom(cf.serializeKey(key)))))
        },
      maxBatchSize,
      groupWithin,
      useCache
    )
  }

  def batchedRawSource(from: ConstraintsBuilder[K], to: ConstraintsBuilder[K])(
    implicit clientOptions: KvdbClientOptions
  ): Source[KvdbBatch, NotUsed] = {
    val range = KeyConstraints.range[K](from, to)
    db.iterateSource(cf, range)
  }

  def batchedSource(implicit clientOptions: KvdbClientOptions): Source[List[(K, V)], NotUsed] = {
    batchedSource(_.first, _.last)
  }

  def batchedSource(from: ConstraintsBuilder[K], to: ConstraintsBuilder[K])(
    implicit clientOptions: KvdbClientOptions
  ): Source[List[(K, V)], NotUsed] = {
    batchedRawSource(from, to)
      .mapAsync(clientOptions.serdesParallelism) { batch =>
        Future {
          batch.view.map(cf.unsafeDeserialize).to(List)
        }
      }
  }

  def source(implicit clientOptions: KvdbClientOptions): Source[(K, V), NotUsed] = {
    source(_.first, _.last)
  }

  def source(from: ConstraintsBuilder[K], to: ConstraintsBuilder[K])(
    implicit clientOptions: KvdbClientOptions
  ): Source[(K, V), NotUsed] = {
    batchedSource(from, to).mapConcat(identity)
  }

  def batchedRawValueSource(from: ConstraintsBuilder[K], to: ConstraintsBuilder[K])(
    implicit clientOptions: KvdbClientOptions
  ): Source[KvdbValueBatch, NotUsed] = {
    val range = KeyConstraints.range[K](from, to)
    db.iterateValuesSource(cf, range)
  }

  def batchedValueSource(from: ConstraintsBuilder[K], to: ConstraintsBuilder[K])(
    implicit clientOptions: KvdbClientOptions
  ): Source[List[V], NotUsed] = {
    batchedRawValueSource(from, to)
      .mapAsync(clientOptions.serdesParallelism) { batch =>
        Future {
          batch.view.map(cf.unsafeDeserializeValue).to(List)
        }
      }
  }

  def valueSource(from: ConstraintsBuilder[K], to: ConstraintsBuilder[K])(
    implicit clientOptions: KvdbClientOptions
  ): Source[V, NotUsed] = {
    batchedValueSource(from, to).mapConcat(identity)
  }

  def batchedValueSource(implicit clientOptions: KvdbClientOptions): Source[List[V], NotUsed] = {
    batchedValueSource(_.first, _.last)
  }

  def valueSource(implicit clientOptions: KvdbClientOptions): Source[V, NotUsed] = {
    valueSource(_.first, _.last)
  }

  def putTask(key: K, value: V): Task[Unit] = {
    val (k, v) = cf.serialize(key, value)
    db.putTask(cf, k, v)
  }

  def putValueTask(value: V)(implicit kt: KeyTransformer[V, K]): Task[Unit] = {
    val (k, v) = cf.serialize(kt.transform(value), value)
    db.putTask(cf, k, v)
  }

  def deleteTask(key: K): Task[Unit] = {
    db.deleteTask(cf, cf.serializeKey(key))
  }

  def transformValueToPairFlow(implicit kt: KeyTransformer[V, K]): Flow[V, (K, V), NotUsed] = {
    Flow[V]
      .map(v => (kt.transform(v), v))
  }

  def putInBatchesFlow(
    sync: Boolean,
    maxBatchSize: Int = 4096,
    groupWithin: FiniteDuration = Duration.Zero,
    batchEncodingParallelism: Int = 2
  ): Flow[(K, V), Seq[(K, V)], NotUsed] = {
    Flow[(K, V)]
      .via(AkkaStreamUtils.batchFlow(maxBatchSize, groupWithin))
      .mapAsync(batchEncodingParallelism) { batch =>
        Future {
          batch
            .foldLeft(db.transactionBuilder()) {
              case (tx, (k, v)) =>
                tx.put(cf, k, v)
            }
            .result
        }.map(serialized => (batch, serialized))
      }
      .mapAsync(1) {
        case (batch, serialized) =>
          db.transactionTask(serialized, sync)
            .map(_ => batch)
            .unsafeRunToFuture
      }
  }

  def putValuesInBatchesFlow(
    sync: Boolean,
    maxBatchSize: Int = 4096,
    groupWithin: FiniteDuration = Duration.Zero,
    batchEncodingParallelism: Int = 2
  )(implicit kt: KeyTransformer[V, K]): Flow[V, Seq[(K, V)], NotUsed] = {
    Flow[V]
      .via(transformValueToPairFlow)
      .via(putInBatchesFlow(sync, maxBatchSize, groupWithin, batchEncodingParallelism))
  }

  def putInBatchesWithCheckpointFlow[CCF <: BCF[CK, CV], CK, CV](
    checkpointColumn: CCF,
    sync: Boolean,
    maxBatchSize: Int = 4096,
    groupWithin: FiniteDuration = Duration.Zero,
    batchEncodingParallelism: Int = 1
  )(
    aggregateCheckpoint: Seq[(K, V)] => (CK, CV)
  ): Flow[(K, V), Seq[(K, V)], NotUsed] = {
    putInBatchesWithCheckpointsFlow[CCF, CK, CV](
      checkpointColumn,
      sync,
      maxBatchSize,
      groupWithin,
      batchEncodingParallelism
    ) { seq => Vector(aggregateCheckpoint(seq)) }
  }

  def putInBatchesWithCheckpointsFlow[CCF <: BCF[CK, CV], CK, CV](
    checkpointColumn: CCF,
    sync: Boolean,
    maxBatchSize: Int = 4096,
    groupWithin: FiniteDuration = Duration.Zero,
    batchEncodingParallelism: Int = 1
  )(
    aggregateCheckpoints: Seq[(K, V)] => Seq[(CK, CV)]
  ): Flow[(K, V), Seq[(K, V)], NotUsed] = {
    Flow[(K, V)]
      .via(AkkaStreamUtils.batchFlow(maxBatchSize, groupWithin))
      .mapAsync(batchEncodingParallelism) { batch =>
        Future {
          val txn = db.transactionBuilder()

          for ((k, v) <- batch) txn.put(cf, k, v)
          for ((ck, cv) <- aggregateCheckpoints(batch)) txn.put(checkpointColumn, ck, cv)

          txn.result
        }.map(serialized => (batch, serialized))
      }
      .mapAsync(1) {
        case (batch, serialized) =>
          db.transactionTask(serialized, sync)
            .as(batch)
            .unsafeRunToFuture
      }
  }

  def putValuesInBatchesWithCheckpointFlow[CCF <: BCF[CK, CV], CK, CV](
    checkpointColumn: CCF,
    sync: Boolean,
    maxBatchSize: Int = 4096,
    groupWithin: FiniteDuration = Duration.Zero,
    batchEncodingParallelism: Int = 1
  )(
    aggregateCheckpoint: Seq[(K, V)] => (CK, CV)
  )(implicit kt: KeyTransformer[V, K]): Flow[V, Seq[(K, V)], NotUsed] = {
    Flow[V]
      .via(transformValueToPairFlow)
      .via(
        putInBatchesWithCheckpointFlow[CCF, CK, CV](
          checkpointColumn,
          sync,
          maxBatchSize,
          groupWithin,
          batchEncodingParallelism
        )(
          aggregateCheckpoint
        )
      )
  }

  def putValuesInBatchesWithCheckpointsFlow[CCF <: BCF[CK, CV], CK, CV](
    checkpointColumn: CCF,
    sync: Boolean,
    maxBatchSize: Int = 4096,
    groupWithin: FiniteDuration = Duration.Zero,
    batchEncodingParallelism: Int = 1
  )(
    aggregateCheckpoints: Seq[(K, V)] => Seq[(CK, CV)]
  )(implicit kt: KeyTransformer[V, K]): Flow[V, Seq[(K, V)], NotUsed] = {
    Flow[V]
      .via(transformValueToPairFlow)
      .via(
        putInBatchesWithCheckpointsFlow[CCF, CK, CV](
          checkpointColumn,
          sync,
          maxBatchSize,
          groupWithin,
          batchEncodingParallelism
        )(
          aggregateCheckpoints
        )
      )
  }

  def tailBatchedRawValueSource(from: ConstraintsBuilder[K], to: ConstraintsBuilder[K])(
    implicit clientOptions: KvdbClientOptions
  ): Source[KvdbValueBatch, NotUsed] = {
    db.tailValueSource(cf, KeyConstraints.range[K](from, to))
      .collect {
        case Right(b) => b
      }
  }

  def tailValueSource(
    from: ConstraintsBuilder[K],
    to: ConstraintsBuilder[K]
  )(implicit clientOptions: KvdbClientOptions): Source[V, NotUsed] = {
    tailBatchedRawValueSource(from, to)
      .mapAsync(clientOptions.serdesParallelism) { b =>
        Future {
          b.view.map(cf.unsafeDeserializeValue).to(List)
        }
      }
      .mapConcat(identity)
  }

  def tailValueSource(implicit clientOptions: KvdbClientOptions): Source[V, NotUsed] = {
    tailValueSource(_.first, _.last)
  }

  def tailBatchedRawSource(
    from: ConstraintsBuilder[K],
    to: ConstraintsBuilder[K]
  )(implicit clientOptions: KvdbClientOptions): Source[KvdbBatch, NotUsed] = {
    db.tailSource(cf, KeyConstraints.range[K](from, to))
      .collect {
        case Right(b) => b
      }
  }

  def tailSource(
    from: ConstraintsBuilder[K],
    to: ConstraintsBuilder[K]
  )(implicit clientOptions: KvdbClientOptions): Source[(K, V), NotUsed] = {
    tailBatchedRawSource(from, to)
      .mapAsync(clientOptions.serdesParallelism) { batch =>
        Future {
          batch.view.map(cf.unsafeDeserialize).to(List)
        }
      }
      .mapConcat(identity)
  }

  def tailSource(implicit clientOptions: KvdbClientOptions): Source[(K, V), NotUsed] = {
    tailSource(_.first, _.last)
  }

  def tailVerboseSource(
    from: ConstraintsBuilder[K],
    to: ConstraintsBuilder[K]
  )(implicit clientOptions: KvdbClientOptions): Source[Either[Instant, (K, V)], NotUsed] = {
    import cats.syntax.either._
    db.tailSource(cf, KeyConstraints.range[K](from, to))
      .mapAsync(clientOptions.serdesParallelism) {
        case Left(e) => Future.successful(List(Either.left(e.time)))
        case Right(batch) =>
          Future {
            batch.view.map(p => Either.right(cf.unsafeDeserialize(p))).to(List)
          }
      }
      .mapConcat(identity)
  }

  def concurrentTailVerboseRawSource(
    ranges: ConstraintsRangesBuilder[K]
  )(implicit clientOptions: KvdbClientOptions): Source[(Int, KvdbTailBatch), NotUsed] = {
    val builtRanges = ranges(KeyConstraints.seed[K]).map {
      case (from, to) =>
        KeyConstraints.toRange[K](from, to)
    }

    db.concurrentTailSource(cf, builtRanges)
  }

  def concurrentTailVerboseSource(
    ranges: ConstraintsRangesBuilder[K]
  )(
    implicit clientOptions: KvdbClientOptions
  ): Source[(Int, Either[Instant, List[(K, V)]]), NotUsed] = {
    import cats.syntax.either._

    concurrentTailVerboseRawSource(ranges)
      .mapAsync(clientOptions.serdesParallelism) {
        case (index, Left(e)) => Future.successful((index, Either.left(e.time)))
        case (index, Right(batch)) =>
          Future {
            val r = Either.right[Instant, List[(K, V)]](batch.view.map(cf.unsafeDeserialize).to(List))
            (index, r)
          }
      }
  }

  def tailVerboseValueSource(
    from: ConstraintsBuilder[K],
    to: ConstraintsBuilder[K]
  )(implicit clientOptions: KvdbClientOptions): Source[Either[Instant, V], NotUsed] = {
    import cats.syntax.either._
    db.tailValueSource(cf, KeyConstraints.range[K](from, to))
      .mapAsync(clientOptions.serdesParallelism) {
        case Left(e) => Future.successful(List(Either.left(e.time)))
        case Right(batch) =>
          Future {
            batch.view.map(p => Either.right(cf.unsafeDeserializeValue(p))).to(List)
          }
      }
      .mapConcat(identity)
  }

  def drop(): Task[Unit] = {
    db.dropColumnFamily(cf)
  }
}
