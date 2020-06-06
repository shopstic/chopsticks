package dev.chopsticks.kvdb.api

import java.time.Instant

import akka.NotUsed
import akka.stream.scaladsl.{Flow, Source}
import com.google.protobuf.ByteString
import dev.chopsticks.fp.ZService
import dev.chopsticks.fp.akka_env.AkkaEnv
import dev.chopsticks.fp.zio_ext._
import dev.chopsticks.kvdb.KvdbWriteTransactionBuilder.TransactionWrite
import dev.chopsticks.kvdb.api.KvdbDatabaseApi.KvdbApiClientOptions
import dev.chopsticks.kvdb.codec.KeyConstraints.{
  ConstraintsBuilder,
  ConstraintsRangesBuilder,
  ConstraintsRangesWithLimitBuilder,
  ConstraintsSeqBuilder
}
import dev.chopsticks.kvdb.codec.{KeyConstraints, KeyTransformer}
import dev.chopsticks.kvdb.proto.KvdbKeyConstraint.Operator
import dev.chopsticks.kvdb.proto.{KvdbKeyConstraint, KvdbKeyConstraintList}
import dev.chopsticks.kvdb.util.KvdbAliases.{KvdbBatch, KvdbPair, KvdbTailBatch, KvdbValueBatch}
import dev.chopsticks.kvdb.{ColumnFamily, KvdbDatabase, KvdbWriteTransactionBuilder}
import dev.chopsticks.stream.AkkaStreamUtils
import eu.timepit.refined.auto._
import eu.timepit.refined.types.numeric.PosInt
import zio.{Task, ZIO}

import scala.collection.immutable.Queue
import scala.collection.mutable
import scala.concurrent.Future
import scala.concurrent.duration._

final class KvdbColumnFamilyApi[BCF[A, B] <: ColumnFamily[A, B], CF <: BCF[K, V], K, V] private[kvdb] (
  db: KvdbDatabase[BCF, _],
  cf: CF,
  options: KvdbApiClientOptions
)(implicit
  rt: zio.Runtime[AkkaEnv]
) {
  private val akkaEnv = ZService.get[AkkaEnv.Service](rt.environment)
  import akkaEnv._

  def withOptions(
    modifier: KvdbApiClientOptions => KvdbApiClientOptions
  ): KvdbColumnFamilyApi[BCF, CF, K, V] = {
    val newOptions = modifier(options)
    new KvdbColumnFamilyApi[BCF, CF, K, V](db.withOptions(newOptions.patchClientOptions), cf, newOptions)
  }

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

  def rawGetRangeTask(from: ConstraintsBuilder[K], to: ConstraintsBuilder[K], limit: PosInt): Task[List[KvdbPair]] = {
    db.getRangeTask(cf, KeyConstraints.range[K](from, to, limit))
  }

  def getRangeTask(from: ConstraintsBuilder[K], to: ConstraintsBuilder[K], limit: PosInt): Task[List[(K, V)]] = {
    rawGetRangeTask(from, to, limit)
      .map(_.map(cf.unsafeDeserialize))
  }

  def batchGetTask(constraints: ConstraintsSeqBuilder[K]): Task[Seq[Option[(K, V)]]] = {
    db.batchGetTask(cf, constraints(KeyConstraints.seed[K]).map(KeyConstraints.toList[K]))
      .map(_.map(_.map(cf.unsafeDeserialize)))
  }

  def batchGetByKeysTask(keys: Seq[K]): Task[Seq[Option[(K, V)]]] = {
    db.batchGetTask(cf, keys.map(k => KeyConstraints.toList(KeyConstraints.seed[K].is(k)(cf.keySerdes))))
      .map(_.map(_.map(cf.unsafeDeserialize)))
  }

  def rawBatchGetRangeTask(ranges: ConstraintsRangesWithLimitBuilder[K]): Task[List[List[KvdbPair]]] = {
    val builtRanges = ranges(KeyConstraints.seed[K]).map {
      case ((from, to), limit) =>
        KeyConstraints.toRange[K](from, to, limit)
    }
    db.batchGetRangeTask(cf, builtRanges)
  }

  def batchGetRangeTask(ranges: ConstraintsRangesWithLimitBuilder[K]): Task[List[List[(K, V)]]] = {
    rawBatchGetRangeTask(ranges)
      .flatMap { groups =>
        ZIO.foreachPar(groups) { g =>
          Task(g.map(cf.unsafeDeserialize))
        }
      }
  }

  def batchGetFlow[O](
    constraintsMapper: O => ConstraintsBuilder[K],
    useCache: Boolean = false
  ): Flow[O, (O, Option[V]), NotUsed] = {
    val maxBatchSize = options.batchWriteMaxBatchSize
    val groupWithin = options.batchWriteBatchingGroupWithin

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
          maxBatchSize.toLong,
          { o: O =>
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
    useCache: Boolean = false
  )(implicit transformer: KeyTransformer[Out, K]): Flow[In, (In, Option[V]), NotUsed] = {
    batchGetFlow(
      (in: In) =>
        _ => {
          val key = transformer.transform(extractor(in))
          KeyConstraints[K](Queue(KvdbKeyConstraint(Operator.EQUAL, ByteString.copyFrom(cf.serializeKey(key)))))
        },
      useCache
    )
  }

  @deprecated("Use rawBatchedSource instead", "2.14")
  def batchedRawSource(from: ConstraintsBuilder[K], to: ConstraintsBuilder[K]): Source[KvdbBatch, NotUsed] = {
    rawBatchedSource(from, to)
  }

  def rawBatchedSource(from: ConstraintsBuilder[K], to: ConstraintsBuilder[K]): Source[KvdbBatch, NotUsed] = {
    val range = KeyConstraints.range[K](from, to)
    db.iterateSource(cf, range)
  }

  def batchedSource: Source[List[(K, V)], NotUsed] = {
    batchedSource(_.first, _.last)
  }

  def batchedSource(from: ConstraintsBuilder[K], to: ConstraintsBuilder[K]): Source[List[(K, V)], NotUsed] = {
    rawBatchedSource(from, to)
      .mapAsync(options.serdesParallelism) { batch =>
        Future {
          batch.view.map(cf.unsafeDeserialize).to(List)
        }
      }
  }

  def watchKeySource(key: K): Source[Option[V], NotUsed] = {
    db.watchKeySource(cf, cf.serializeKey(key))
      .mapAsync(options.serdesParallelism) { value =>
        Future {
          value.map(cf.unsafeDeserializeValue)
        }
      }
  }

  def source: Source[(K, V), NotUsed] = {
    source(_.first, _.last)
  }

  def source(from: ConstraintsBuilder[K], to: ConstraintsBuilder[K]): Source[(K, V), NotUsed] = {
    batchedSource(from, to).mapConcat(identity)
  }

  @deprecated("Use rawBatchedValueSource instead", "2.14")
  def batchedRawValueSource(from: ConstraintsBuilder[K], to: ConstraintsBuilder[K]): Source[KvdbValueBatch, NotUsed] = {
    rawBatchedValueSource(from, to)
  }

  def rawBatchedValueSource(from: ConstraintsBuilder[K], to: ConstraintsBuilder[K]): Source[KvdbValueBatch, NotUsed] = {
    val range = KeyConstraints.range[K](from, to)
    db.iterateSource(cf, range)
      .map(_.map(_._2))
  }

  def batchedValueSource(from: ConstraintsBuilder[K], to: ConstraintsBuilder[K]): Source[List[V], NotUsed] = {
    rawBatchedValueSource(from, to)
      .mapAsync(options.serdesParallelism) { batch =>
        Future {
          batch.view.map(cf.unsafeDeserializeValue).to(List)
        }
      }
  }

  def valueSource(from: ConstraintsBuilder[K], to: ConstraintsBuilder[K]): Source[V, NotUsed] = {
    batchedValueSource(from, to).mapConcat(identity)
  }

  def batchedValueSource: Source[List[V], NotUsed] = {
    batchedValueSource(_.first, _.last)
  }

  def valueSource: Source[V, NotUsed] = {
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

  def batchPut(batch: Vector[(K, V)]): KvdbWriteTransactionBuilder[BCF] = {
    val tx = db.transactionBuilder()
    for ((k, v) <- batch) {
      val _ = tx.put(cf, k, v)
    }
    tx
  }

  type BatchPutTxBuilder[P] = Vector[(K, V)] => (List[TransactionWrite], P)

  def putPairsInBatchesFlow[P](
    buildTransaction: BatchPutTxBuilder[P]
  ): Flow[(K, V), P, NotUsed] = {
    Flow[(K, V)]
      .via(AkkaStreamUtils.batchFlow(options.batchWriteMaxBatchSize, options.batchWriteBatchingGroupWithin))
      .mapAsync(options.serdesParallelism) { batch =>
        Future {
          buildTransaction(batch)
        }
      }
      .mapAsync(1) {
        case (writes, passthrough) =>
          db.transactionTask(writes)
            .as(passthrough)
            .unsafeRunToFuture
      }
  }

  def putPairsInBatchesFlow: Flow[(K, V), Vector[(K, V)], NotUsed] = {
    putPairsInBatchesFlow(batch => {
      batchPut(batch).result -> batch
    })
  }

  def putValuesInBatchesFlow[P](
    buildTransaction: BatchPutTxBuilder[P]
  )(implicit kt: KeyTransformer[V, K]): Flow[V, P, NotUsed] = {
    Flow[V]
      .via(transformValueToPairFlow)
      .via(putPairsInBatchesFlow(buildTransaction))
  }

  def putValuesInBatchesFlow(implicit kt: KeyTransformer[V, K]): Flow[V, Vector[(K, V)], NotUsed] = {
    putValuesInBatchesFlow[Vector[(K, V)]](batch => {
      batchPut(batch).result -> batch
    })
  }

  @deprecated("Use rawTailBatchedValueSource instead", "2.14")
  def tailBatchedRawValueSource(
    from: ConstraintsBuilder[K],
    to: ConstraintsBuilder[K]
  ): Source[KvdbValueBatch, NotUsed] = {
    rawTailBatchedValueSource(from, to)
  }

  def rawTailBatchedValueSource(
    from: ConstraintsBuilder[K],
    to: ConstraintsBuilder[K]
  ): Source[KvdbValueBatch, NotUsed] = {
    db.tailSource(cf, KeyConstraints.range[K](from, to))
      .collect {
        case Right(b) => b.map(_._2)
      }
  }

  def tailValueSource(
    from: ConstraintsBuilder[K],
    to: ConstraintsBuilder[K]
  ): Source[V, NotUsed] = {
    rawTailBatchedValueSource(from, to)
      .mapAsync(options.serdesParallelism) { b =>
        Future {
          b.view.map(cf.unsafeDeserializeValue).to(List)
        }
      }
      .mapConcat(identity)
  }

  def tailValueSource: Source[V, NotUsed] = {
    tailValueSource(_.first, _.last)
  }

  @deprecated("Use rawTailBatchedSource instead", "2.14")
  def tailBatchedRawSource(
    from: ConstraintsBuilder[K],
    to: ConstraintsBuilder[K]
  ): Source[KvdbBatch, NotUsed] = {
    rawTailBatchedSource(from, to)
  }

  def rawTailBatchedSource(
    from: ConstraintsBuilder[K],
    to: ConstraintsBuilder[K]
  ): Source[KvdbBatch, NotUsed] = {
    db.tailSource(cf, KeyConstraints.range[K](from, to))
      .collect {
        case Right(b) => b
      }
  }

  def tailBatchedSource(
    from: ConstraintsBuilder[K],
    to: ConstraintsBuilder[K]
  ): Source[List[(K, V)], NotUsed] = {
    rawTailBatchedSource(from, to)
      .mapAsync(options.serdesParallelism) { batch =>
        Future {
          batch.view.map(cf.unsafeDeserialize).to(List)
        }
      }
  }

  def tailSource(
    from: ConstraintsBuilder[K],
    to: ConstraintsBuilder[K]
  ): Source[(K, V), NotUsed] = {
    tailBatchedSource(from, to)
      .mapConcat(identity)
  }

  def tailSource: Source[(K, V), NotUsed] = {
    tailSource(_.first, _.last)
  }

  def tailVerboseSource(
    from: ConstraintsBuilder[K],
    to: ConstraintsBuilder[K]
  ): Source[Either[Instant, (K, V)], NotUsed] = {
    import cats.syntax.either._
    db.tailSource(cf, KeyConstraints.range[K](from, to))
      .mapAsync(options.serdesParallelism) {
        case Left(e) => Future.successful(List(Either.left(e.time)))
        case Right(batch) =>
          Future {
            batch.view.map(p => Either.right(cf.unsafeDeserialize(p))).to(List)
          }
      }
      .mapConcat(identity)
  }

  @deprecated("Use rawConcurrentTailVerboseSource instead", "2.14")
  def concurrentTailVerboseRawSource(
    ranges: ConstraintsRangesBuilder[K]
  ): Source[(Int, KvdbTailBatch), NotUsed] = {
    rawConcurrentTailVerboseSource(ranges)
  }

  def rawConcurrentTailVerboseSource(
    ranges: ConstraintsRangesBuilder[K]
  ): Source[(Int, KvdbTailBatch), NotUsed] = {
    val builtRanges = ranges(KeyConstraints.seed[K]).map {
      case (from, to) =>
        KeyConstraints.toRange[K](from, to)
    }

    db.concurrentTailSource(cf, builtRanges)
  }

  def concurrentTailVerboseSource(
    ranges: ConstraintsRangesBuilder[K]
  ): Source[(Int, Either[Instant, List[(K, V)]]), NotUsed] = {
    import cats.syntax.either._

    rawConcurrentTailVerboseSource(ranges)
      .mapAsync(options.serdesParallelism) {
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
  ): Source[Either[Instant, V], NotUsed] = {
    import cats.syntax.either._
    db.tailSource(cf, KeyConstraints.range[K](from, to))
      .mapAsync(options.serdesParallelism) {
        case Left(e) => Future.successful(List(Either.left(e.time)))
        case Right(batch) =>
          Future {
            batch.view.map(p => Either.right(cf.unsafeDeserializeValue(p._2))).to(List)
          }
      }
      .mapConcat(identity)
  }

  def drop(): Task[Unit] = {
    db.dropColumnFamily(cf)
  }
}
