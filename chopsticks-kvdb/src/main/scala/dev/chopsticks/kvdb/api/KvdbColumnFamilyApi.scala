package dev.chopsticks.kvdb.api

import java.time.Instant
import com.google.protobuf.ByteString
import dev.chopsticks.kvdb.KvdbWriteTransactionBuilder.TransactionWrite
import dev.chopsticks.kvdb.codec.KeyConstraints.{
  ConstraintsBuilder,
  ConstraintsRangesBuilder,
  ConstraintsRangesWithLimitBuilder,
  ConstraintsSeqBuilder
}
import dev.chopsticks.kvdb.codec.{KeyConstraints, KeySerdes, KeyTransformer}
import dev.chopsticks.kvdb.proto.KvdbKeyConstraint.Operator
import dev.chopsticks.kvdb.proto.{KvdbKeyConstraint, KvdbKeyConstraintList}
import dev.chopsticks.kvdb.util.KvdbAliases.{KvdbBatch, KvdbPair, KvdbTailBatch, KvdbValueBatch}
import dev.chopsticks.kvdb.util.KvdbSerdesThreadPool
import dev.chopsticks.kvdb.{ColumnFamily, KvdbDatabase, KvdbWriteTransactionBuilder}
import dev.chopsticks.kvdb.api.KvdbDatabaseApi.KvdbApiClientOptions
import dev.chopsticks.stream.StreamUtils
import eu.timepit.refined.auto.*
import eu.timepit.refined.types.numeric.PosInt
import zio.{Chunk, Task, ZIO}
import zio.stream.*

import scala.collection.concurrent.TrieMap
import scala.collection.immutable.Queue
import scala.collection.mutable

final class KvdbColumnFamilyApi[CF <: ColumnFamily[_, _], CFS <: ColumnFamily[_, _]] private[kvdb] (
  val db: KvdbDatabase[CFS],
  val cf: CF,
  val options: KvdbApiClientOptions
)(using CFS <:< CF)(implicit
  rt: zio.Runtime[KvdbSerdesThreadPool]
):
  private val serdesThreadPool = rt.environment.get[KvdbSerdesThreadPool]

  implicit private val keySerdes: KeySerdes[cf.Key] = cf.keySerdes

  def withOptions(
    modifier: KvdbApiClientOptions => KvdbApiClientOptions
  ): KvdbColumnFamilyApi[CF, CFS] =
    val newOptions = modifier(options)
    new KvdbColumnFamilyApi[CF, CFS](db.withOptions(newOptions.patchClientOptions), cf, newOptions)

  def estimateCountTask: Task[Long] =
    db.estimateCount(cf)

  def getTask(constraints: ConstraintsBuilder[cf.Key]): Task[Option[(cf.Key, cf.Value)]] = {
    db
      .getTask(cf, KeyConstraints.build(constraints))
      .flatMap(r => ZIO.attempt(r.map(cf.unsafeDeserialize)).onExecutor(serdesThreadPool.executor))
  }

  def getKeyTask(constraints: ConstraintsBuilder[cf.Key]): Task[Option[cf.Key]] =
    db
      .getTask(cf, KeyConstraints.build(constraints))
      .flatMap(r => ZIO.attempt(r.map(p => cf.unsafeDeserializeKey(p._1))).onExecutor(serdesThreadPool.executor))

  def getValueTask(constraints: ConstraintsBuilder[cf.Key]): Task[Option[cf.Value]] =
    db
      .getTask(cf, KeyConstraints.build(constraints))
      .flatMap(r => ZIO.attempt(r.map(p => cf.unsafeDeserializeValue(p._2))).onExecutor(serdesThreadPool.executor))

  def rawGetRangeTask(
    from: ConstraintsBuilder[cf.Key],
    to: ConstraintsBuilder[cf.Key],
    limit: PosInt,
    reverse: Boolean
  ): Task[List[KvdbPair]] =
    db.getRangeTask(cf, KeyConstraints.range[cf.Key](from, to, limit), reverse)

  def getRangeTask(
    from: ConstraintsBuilder[cf.Key],
    to: ConstraintsBuilder[cf.Key],
    limit: PosInt,
    reverse: Boolean
  ): Task[List[(cf.Key, cf.Value)]] =
    rawGetRangeTask(from, to, limit, reverse)
      .flatMap(xs => ZIO.attempt(xs.map(cf.unsafeDeserialize)).onExecutor(serdesThreadPool.executor))

  def batchGetTask(constraints: ConstraintsSeqBuilder[cf.Key]): Task[Seq[Option[(cf.Key, cf.Value)]]] =
    db.batchGetTask(cf, constraints(KeyConstraints.seed[cf.Key]).map(KeyConstraints.toList[cf.Key]))
      .flatMap(xs => ZIO.attempt(xs.map(_.map(cf.unsafeDeserialize))).onExecutor(serdesThreadPool.executor))

  def batchGetByKeysTask(keys: Seq[cf.Key]): Task[Seq[Option[(cf.Key, cf.Value)]]] =
    db.batchGetTask(cf, keys.map(k => KeyConstraints.toList(KeyConstraints.seed[cf.Key].is(k))))
      .flatMap(r => ZIO.attempt(r.map(_.map(cf.unsafeDeserialize))).onExecutor(serdesThreadPool.executor))

  def rawBatchGetRangeTask(ranges: ConstraintsRangesWithLimitBuilder[cf.Key]): Task[Seq[List[KvdbPair]]] =
    val builtRanges = ranges(KeyConstraints.seed[cf.Key]).map {
      case ((from, to), limit) =>
        KeyConstraints.toRange[cf.Key](from, to, limit)
    }
    db.batchGetRangeTask(cf, builtRanges, reverse = false)

  def batchGetRangeTask(ranges: ConstraintsRangesWithLimitBuilder[cf.Key]): Task[Seq[List[(cf.Key, cf.Value)]]] = {
    rawBatchGetRangeTask(ranges)
      .flatMap { groups =>
        ZIO.foreachPar(groups) { g =>
          ZIO.attempt(g.map(cf.unsafeDeserialize)).onExecutor(serdesThreadPool.executor)
        }
      }
  }

  @deprecated()
  def getFlow[O](
    constraintsMapper: O => ConstraintsBuilder[cf.Key],
    useUnboundedCache: Boolean = false,
    unordered: Boolean = false
  ): ZPipeline[Any, Throwable, O, (O, Option[cf.Value])] =
    getPipeline(constraintsMapper, useUnboundedCache, unordered)

  def getPipeline[O](
    constraintsMapper: O => ConstraintsBuilder[cf.Key],
    useUnboundedCache: Boolean = false,
    unordered: Boolean = false
  ): ZPipeline[Any, Throwable, O, (O, Option[cf.Value])] =
    batchedGetFlow(constraintsMapper, useUnboundedCache, unordered) >>>
      StreamUtils.Pipeline.mapConcatChunk((x: Chunk[(O, Option[cf.Value])]) => x)

  def batchedGetFlow[O](
    constraintsMapper: O => ConstraintsBuilder[cf.Key],
    useUnboundedCache: Boolean = false,
    unordered: Boolean = false
  ): ZPipeline[Any, Throwable, O, Chunk[(O, Option[cf.Value])]] = {
    val batchPipeline =
      StreamUtils.batchPipeline[O](options.batchWriteMaxBatchSize, options.batchWriteBatchingGroupWithin)

    val processBatch: Chunk[O] => Task[Chunk[(O, Option[cf.Value])]] =
      if (useUnboundedCache) {
        val cache = TrieMap.empty[KvdbKeyConstraintList, Option[cf.Value]]

        (keyBatch: Chunk[O]) => {
          for {
            requests <- ZIO.succeed {
              keyBatch.map(k => KeyConstraints.toList(constraintsMapper(k)(KeyConstraints.seed[cf.Key])))
            }
            results <- {
              val maybeValues = new Array[Option[cf.Value]](requests.size)
              val uncachedIndicesBuilder = mutable.ArrayBuilder.make[Int]
              val uncachedGetsBuilder = mutable.ArrayBuilder.make[KvdbKeyConstraintList]

              for ((request, index) <- requests.zipWithIndex)
                cache.get(request) match
                  case Some(v) => maybeValues(index) = v
                  case None =>
                    val _ = uncachedIndicesBuilder += index
                    val _ = uncachedGetsBuilder += request

              val uncachedRequests = uncachedGetsBuilder.result()

              if (uncachedRequests.isEmpty)
                ZIO.succeed(keyBatch.zip(maybeValues))
              else
                val uncachedIndices = uncachedIndicesBuilder.result()
                db.batchGetTask(cf, uncachedRequests.toList)
                  .flatMap { pairs =>
                    ZIO.attempt {
                      for ((maybePair, index) <- pairs.zipWithIndex) {
                        val maybeValue = maybePair.map(p => cf.unsafeDeserializeValue(p._2))
                        maybeValues(uncachedIndices(index)) = maybeValue
                        cache.update(uncachedRequests(index), maybeValue)
                      }
                      keyBatch.zip(maybeValues)
                    }.onExecutor(serdesThreadPool.executor)
                  }
            }
          } yield results
        }
      }
      else {
        (keyBatch: Chunk[O]) =>
          for {
            requests <-
              ZIO.succeed(keyBatch.map(k => KeyConstraints.toList(constraintsMapper(k)(KeyConstraints.seed[cf.Key]))))
            results <- db.batchGetTask(cf, requests)
              .flatMap { maybePairs =>
                ZIO
                  .attempt {
                    val result: Chunk[(O, Option[cf.Value])] =
                      keyBatch.zip(maybePairs.map(_.map(p => cf.unsafeDeserializeValue(p._2))))
                    result
                  }
                  .onExecutor(serdesThreadPool.executor)
              }
          } yield results
      }

    val parallelism = options.batchWriteParallelism.value
    if (unordered) batchPipeline.mapZIOParUnordered(parallelism)(xs => processBatch(xs))
    else batchPipeline.mapZIOPar(parallelism)(xs => processBatch(xs))
  }

  @deprecated(
    "batchGetByKeysFlow will be removed in future versions, use getByKeysFlow or batchedGetByKeysFlow instead.",
    "3.3.0"
  )
  def batchGetByKeysFlow[In, Out](
    extractor: In => Out,
    useCache: Boolean = false
  )(implicit transformer: KeyTransformer[Out, cf.Key]): ZPipeline[Any, Throwable, In, (In, Option[cf.Value])] =
    getByKeysFlow(extractor, useCache)

  def getByKeysFlow[In, Out](
    extractor: In => Out,
    useCache: Boolean = false,
    unordered: Boolean = false
  )(implicit transformer: KeyTransformer[Out, cf.Key]): ZPipeline[Any, Throwable, In, (In, Option[cf.Value])] =
    batchedGetByKeysFlow(extractor, useCache, unordered) >>>
      StreamUtils.Pipeline.mapConcatChunk((x: Chunk[(In, Option[cf.Value])]) => x)

  def batchedGetByKeysFlow[In, Out](
    extractor: In => Out,
    useCache: Boolean = false,
    unordered: Boolean = false
  )(implicit transformer: KeyTransformer[Out, cf.Key]): ZPipeline[Any, Throwable, In, Chunk[(In, Option[cf.Value])]] =
    batchedGetFlow(
      (in: In) =>
        _ => {
          val key = transformer.transform(extractor(in))
          KeyConstraints[cf.Key](Queue(KvdbKeyConstraint(Operator.EQUAL, ByteString.copyFrom(cf.serializeKey(key)))))
        },
      useCache,
      unordered
    )

  @deprecated("Use rawBatchedSource instead", "2.14")
  def batchedRawSource(
    from: ConstraintsBuilder[cf.Key],
    to: ConstraintsBuilder[cf.Key]
  ): Stream[Throwable, KvdbBatch] =
    rawBatchedSource(from, to)

  def rawBatchedSource(from: ConstraintsBuilder[cf.Key], to: ConstraintsBuilder[cf.Key]): Stream[Throwable, KvdbBatch] =
    val range = KeyConstraints.range[cf.Key](from, to)
    db.iterateSource(cf, range)

  def batchedSource: Stream[Throwable, Chunk[(cf.Key, cf.Value)]] =
    batchedSource(_.first, _.last)

  def batchedSource(
    from: ConstraintsBuilder[cf.Key],
    to: ConstraintsBuilder[cf.Key]
  ): Stream[Throwable, Chunk[(cf.Key, cf.Value)]] =
    rawBatchedSource(from, to)
      .mapZIOPar(options.serdesParallelism) { batch =>
        ZIO
          .attempt(batch.map(cf.unsafeDeserialize))
          .onExecutor(serdesThreadPool.executor)
      }

//  def watchKeySource(key: cf.Key): Source[Option[V], Future[Done]] =
//    db.watchKeySource(cf, cf.serializeKey(key))
//      .mapAsync(options.serdesParallelism) { value =>
//        Future {
//          value.map(cf.unsafeDeserializeValue)
//        }(serdesThreadPool.executionContext)
//      }

  def source: Stream[Throwable, (cf.Key, cf.Value)] =
    source(_.first, _.last)

  def source(
    from: ConstraintsBuilder[cf.Key],
    to: ConstraintsBuilder[cf.Key]
  ): Stream[Throwable, (cf.Key, cf.Value)] =
    batchedSource(from, to).mapConcat(identity)

  @deprecated("Use rawBatchedValueSource instead", "2.14")
  def batchedRawValueSource(
    from: ConstraintsBuilder[cf.Key],
    to: ConstraintsBuilder[cf.Key]
  ): Stream[Throwable, KvdbValueBatch] =
    rawBatchedValueSource(from, to)

  def rawBatchedValueSource(
    from: ConstraintsBuilder[cf.Key],
    to: ConstraintsBuilder[cf.Key]
  ): Stream[Throwable, KvdbValueBatch] =
    val range = KeyConstraints.range[cf.Key](from, to)
    db
      .iterateSource(cf, range)
      .map(_.map(_._2))

  def batchedValueSource(
    from: ConstraintsBuilder[cf.Key],
    to: ConstraintsBuilder[cf.Key]
  ): Stream[Throwable, Chunk[cf.Value]] =
    rawBatchedValueSource(from, to)
      // todo possibly we should rechunk afterwards
      .mapZIOPar(options.serdesParallelism) { batch =>
        ZIO
          .attempt(batch.map(cf.unsafeDeserializeValue))
          .onExecutor(serdesThreadPool.executor)
      }

  def valueSource(from: ConstraintsBuilder[cf.Key], to: ConstraintsBuilder[cf.Key]): Stream[Throwable, cf.Value] =
    batchedValueSource(from, to).mapConcat(identity)

  def batchedValueSource: Stream[Throwable, Chunk[cf.Value]] =
    batchedValueSource(_.first, _.last)

  def valueSource: Stream[Throwable, cf.Value] =
    valueSource(_.first, _.last)

  def putTask(key: cf.Key, value: cf.Value): Task[Unit] =
    for
      kv <- ZIO.attempt(cf.serialize(key, value)).onExecutor(serdesThreadPool.executor)
      (k, v) = kv
      _ <- db.putTask(cf, k, v)
    yield ()

  def putValueTask(value: cf.Value)(implicit kt: KeyTransformer[cf.Value, cf.Key]): Task[Unit] =
    for
      kv <- ZIO.attempt(cf.serialize(kt.transform(value), value)).onExecutor(serdesThreadPool.executor)
      (k, v) = kv
      _ <- db.putTask(cf, k, v)
    yield ()

  def deleteTask(key: cf.Key): Task[Unit] =
    for
      k <- ZIO.attempt(cf.serializeKey(key)).onExecutor(serdesThreadPool.executor)
      _ <- db.deleteTask(cf, k)
    yield ()

  def transformValueToPairFlow(implicit
    kt: KeyTransformer[cf.Value, cf.Key]
  ): ZPipeline[Any, Nothing, cf.Value, (cf.Key, cf.Value)] =
    ZPipeline.map(v => (kt.transform(v), v))

  def batchPut(batch: Chunk[(cf.Key, cf.Value)]): KvdbWriteTransactionBuilder[CFS] =
    val tx = db.transactionBuilder()
    for ((k, v) <- batch) {
      val _ = tx.put(cf, k, v)
    }
    tx

  type BatchPutTxBuilder[P] = Chunk[(cf.Key, cf.Value)] => (List[TransactionWrite], P)

  def putPairsInBatchesFlow[P](
    buildTransaction: BatchPutTxBuilder[P]
  ): ZPipeline[Any, Throwable, (cf.Key, cf.Value), P] =
    StreamUtils
      .batchPipeline(options.batchWriteMaxBatchSize, options.batchWriteBatchingGroupWithin)
      .mapZIOPar(options.serdesParallelism) { (batch: Chunk[(cf.Key, cf.Value)]) =>
        ZIO
          .attempt(buildTransaction(batch))
          .onExecutor(serdesThreadPool.executor)
      }
      .mapZIOPar(1) { case (writes: List[TransactionWrite] @unchecked, passthrough: P @unchecked) =>
        db
          .transactionTask(writes)
          .as(passthrough)

      }

  def putPairsInBatchesFlow: ZPipeline[Any, Throwable, (cf.Key, cf.Value), Chunk[(cf.Key, cf.Value)]] =
    putPairsInBatchesFlow(batch =>
      batchPut(batch).result -> batch
    )

  def putValuesInBatchesFlow[P](
    buildTransaction: BatchPutTxBuilder[P]
  )(implicit kt: KeyTransformer[cf.Value, cf.Key]): ZPipeline[Any, Throwable, cf.Value, P] =
    transformValueToPairFlow(kt) >>>
      putPairsInBatchesFlow(buildTransaction)

  def putValuesInBatchesFlow(implicit
    kt: KeyTransformer[cf.Value, cf.Key]
  ): ZPipeline[Any, Throwable, cf.Value, Chunk[(cf.Key, cf.Value)]] =
    putValuesInBatchesFlow[Chunk[(cf.Key, cf.Value)]](batch =>
      batchPut(batch).result -> batch
    )

  @deprecated("Use rawTailBatchedValueSource instead", "2.14")
  def tailBatchedRawValueSource(
    from: ConstraintsBuilder[cf.Key],
    to: ConstraintsBuilder[cf.Key]
  ): Stream[Throwable, KvdbValueBatch] =
    rawTailBatchedValueSource(from, to)

  def rawTailBatchedValueSource(
    from: ConstraintsBuilder[cf.Key],
    to: ConstraintsBuilder[cf.Key]
  ): Stream[Throwable, KvdbValueBatch] =
    db
      .tailSource(cf, KeyConstraints.range[cf.Key](from, to))
      .collect { case Right(b) => b.map(_._2) }

  def tailValueSource(
    from: ConstraintsBuilder[cf.Key],
    to: ConstraintsBuilder[cf.Key]
  ): Stream[Throwable, cf.Value] =
    rawTailBatchedValueSource(from, to)
      .mapZIOPar(options.serdesParallelism) { b =>
        ZIO
          .attempt(b.map(cf.unsafeDeserializeValue))
          .onExecutor(serdesThreadPool.executor)
      }
      .mapConcat(identity)

  def tailValueSource: Stream[Throwable, cf.Value] =
    tailValueSource(_.first, _.last)

  @deprecated("Use rawTailBatchedSource instead", "2.14")
  def tailBatchedRawSource(
    from: ConstraintsBuilder[cf.Key],
    to: ConstraintsBuilder[cf.Key]
  ): Stream[Throwable, KvdbBatch] =
    rawTailBatchedSource(from, to)

  def rawTailBatchedSource(
    from: ConstraintsBuilder[cf.Key],
    to: ConstraintsBuilder[cf.Key]
  ): Stream[Throwable, KvdbBatch] =
    db
      .tailSource(cf, KeyConstraints.range[cf.Key](from, to))
      .collect { case Right(b) => b }

  def tailBatchedSource(
    from: ConstraintsBuilder[cf.Key],
    to: ConstraintsBuilder[cf.Key]
  ): Stream[Throwable, Chunk[(cf.Key, cf.Value)]] =
    rawTailBatchedSource(from, to)
      .mapZIOPar(options.serdesParallelism) { batch =>
        ZIO
          .attempt { batch.map(cf.unsafeDeserialize) }
          .onExecutor(serdesThreadPool.executor)
      }

  def tailSource(
    from: ConstraintsBuilder[cf.Key],
    to: ConstraintsBuilder[cf.Key]
  ): Stream[Throwable, (cf.Key, cf.Value)] =
    tailBatchedSource(from, to)
      .mapConcat(identity)

  def tailSource: Stream[Throwable, (cf.Key, cf.Value)] =
    tailSource(_.first, _.last)

  def tailVerboseSource(
    from: ConstraintsBuilder[cf.Key],
    to: ConstraintsBuilder[cf.Key]
  ): Stream[Throwable, Either[Instant, (cf.Key, cf.Value)]] = {
    db
      .tailSource(cf, KeyConstraints.range[cf.Key](from, to))
      // todo possibly rechunk afterwards
      .mapZIOPar(options.serdesParallelism) {
        case Left(e) => ZIO.succeed(Chunk.single(Left(e.time)))
        case Right(batch) =>
          ZIO
            .attempt(batch.map(p => Right(cf.unsafeDeserialize(p))))
            .onExecutor(serdesThreadPool.executor)
      }
      .mapConcat(identity)
  }

  def tailBatchedVerboseSource(
    from: ConstraintsBuilder[cf.Key],
    to: ConstraintsBuilder[cf.Key]
  ): Stream[Throwable, Either[Instant, Chunk[(cf.Key, cf.Value)]]] =
    db
      .tailSource(cf, KeyConstraints.range[cf.Key](from, to))
      // todo possibly rechunk afterwards
      .mapZIOPar(options.serdesParallelism) {
        case Left(e) => ZIO.succeed(Left(e.time))
        case Right(batch) =>
          ZIO
            .attempt(Right(batch.map(p => cf.unsafeDeserialize(p))))
            .onExecutor(serdesThreadPool.executor)
      }

  @deprecated("Use rawConcurrentTailVerboseSource instead", "2.14")
  def concurrentTailVerboseRawSource(
    ranges: ConstraintsRangesBuilder[cf.Key]
  ): Stream[Throwable, (Int, KvdbTailBatch)] =
    rawConcurrentTailVerboseSource(ranges)

  def rawConcurrentTailVerboseSource(
    ranges: ConstraintsRangesBuilder[cf.Key]
  ): Stream[Throwable, (Int, KvdbTailBatch)] =
    val builtRanges = ranges(KeyConstraints.seed[cf.Key]).map {
      case (from, to) =>
        KeyConstraints.toRange[cf.Key](from, to)
    }
    db.concurrentTailSource(cf, builtRanges)

  def concurrentTailVerboseSource(
    ranges: ConstraintsRangesBuilder[cf.Key]
  ): Stream[Throwable, (Int, Either[Instant, Chunk[(cf.Key, cf.Value)]])] =
    rawConcurrentTailVerboseSource(ranges)
      .mapZIOPar(options.serdesParallelism) {
        case (index, Left(e)) => ZIO.succeed((index, Left(e.time)))
        case (index, Right(batch)) =>
          ZIO
            .attempt {
              val r = Right(batch.map(cf.unsafeDeserialize))
              (index, r)
            }
            .onExecutor(serdesThreadPool.executor)
      }

  def tailVerboseValueSource(
    from: ConstraintsBuilder[cf.Key],
    to: ConstraintsBuilder[cf.Key]
  ): Stream[Throwable, Either[Instant, cf.Value]] =
    db
      .tailSource(cf, KeyConstraints.range[cf.Key](from, to))
      .mapZIOPar(options.serdesParallelism) {
        case Left(e) => ZIO.succeed(List(Left(e.time)))
        case Right(batch) =>
          ZIO
            .attempt(batch.map(p => Right(cf.unsafeDeserializeValue(p._2))))
            .onExecutor(serdesThreadPool.executor)
      }
      .mapConcat(identity)

  def drop(): Task[Unit] =
    db.dropColumnFamily(cf)

end KvdbColumnFamilyApi
