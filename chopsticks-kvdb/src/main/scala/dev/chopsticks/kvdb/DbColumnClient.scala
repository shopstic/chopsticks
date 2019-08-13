package dev.chopsticks.kvdb

import java.time.Instant

import akka.NotUsed
import akka.stream.scaladsl.{Flow, Source}
import com.google.protobuf.ByteString
import dev.chopsticks.kvdb.codec.DbKeyConstraints.{ConstraintsBuilder, ConstraintsRangesBuilder, ConstraintsSeqBuilder}
import dev.chopsticks.kvdb.codec.{DbKeyConstraints, DbKeyTransformer}
import dev.chopsticks.kvdb.DbInterface.DbDefinition
import dev.chopsticks.fp.AkkaEnv
import dev.chopsticks.kvdb.util.DbUtils.{DbClientOptions, DbTailBatch}
import dev.chopsticks.kvdb.proto.DbKeyConstraint.Operator
import dev.chopsticks.kvdb.proto._
import dev.chopsticks.stream.AkkaStreamUtils
import zio.Task

import scala.collection.immutable.Queue
import scala.collection.{breakOut, mutable}
import scala.concurrent.Future
import scala.concurrent.duration._
import scala.language.higherKinds

final class DbColumnClient[DbDef <: DbDefinition, Col[A, B] <: DbDef#BaseCol[A, B], K, V](
  db: DbInterface[DbDef],
  column: Col[K, V]
)(
  implicit akkaEnv: AkkaEnv
) {
  import akkaEnv._

  def estimateCountTask: Task[Long] = {
    db.estimateCount(column)
  }

  def getTask(constraints: ConstraintsBuilder[K]): Task[Option[(K, V)]] = {
    db.getTask(column, DbKeyConstraints.build(constraints))
      .map(_.map(column.unsafeDecode))
  }

  def getKeyTask(constraints: ConstraintsBuilder[K]): Task[Option[K]] = {
    db.getTask(column, DbKeyConstraints.build(constraints))
      .map(_.map(p => column.unsafeDecodeKey(p._1)))
  }

  def getValueTask(constraints: ConstraintsBuilder[K]): Task[Option[V]] = {
    db.getTask(column, DbKeyConstraints.build(constraints))
      .map(_.map(p => column.unsafeDecodeValue(p._2)))
  }

  def batchGetTask(constraints: ConstraintsSeqBuilder[K]): Task[Seq[Option[(K, V)]]] = {
    db.batchGetTask(column, constraints(DbKeyConstraints.seed[K]).map(DbKeyConstraints.toList[K]))
      .map(_.map(_.map(column.unsafeDecode)))
  }

  def batchGetByKeysTask(keys: Seq[K]): Task[Seq[Option[(K, V)]]] = {
    db.batchGetTask(column, keys.map(k => DbKeyConstraints.toList(DbKeyConstraints.seed[K].is(k)(column.dbKey))))
      .map(_.map(_.map(column.unsafeDecode)))
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
          (keyBatch, keyBatch.map(k => DbKeyConstraints.toList(constraintsMapper(k)(DbKeyConstraints.seed[K]))))
        }
    }
    else {
      Flow[O]
        .batch(
          maxBatchSize.toLong, { o: O =>
            val keyBatch = Vector.newBuilder[O]
            val valueBatch = Vector.newBuilder[DbKeyConstraintList]
            (keyBatch += o, valueBatch += DbKeyConstraints.toList(constraintsMapper(o)(DbKeyConstraints.seed[K])))
          }
        ) {
          case ((keyBatch, valueBatch), o) =>
            (keyBatch += o, valueBatch += DbKeyConstraints.toList(constraintsMapper(o)(DbKeyConstraints.seed[K])))
        }
        .map {
          case (keyBatch, valueBatch) =>
            (keyBatch.result(), valueBatch.result())
        }
    }

    flow
      .via(AkkaStreamUtils.statefulMapOptionFlow(() => {
        val cache = mutable.Map.empty[DbKeyConstraintList, Option[V]]

        {
          case (keyBatch: Seq[O], requests: Seq[DbKeyConstraintList]) =>
            val future = if (useCache) {
              val maybeValues = new Array[Option[V]](requests.size)
              val uncachedIndicesBuilder = mutable.ArrayBuilder.make[Int]
              val uncachedGetsBuilder = mutable.ArrayBuilder.make[DbKeyConstraintList]

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

                unsafeRunToFuture(
                  db.batchGetTask(column, uncachedRequests.toList)
                    .map { pairs =>
                      for ((maybePair, index) <- pairs.zipWithIndex) {
                        val maybeValue = maybePair.map(p => column.unsafeDecodeValue(p._2))
                        maybeValues(uncachedIndices(index)) = maybeValue
                        cache.update(uncachedRequests(index), maybeValue)
                      }

                      keyBatch.zip(maybeValues)
                    }
                )
              }
            }
            else {
              unsafeRunToFuture(
                db.batchGetTask(column, requests)
                  .map { maybePairs =>
                    keyBatch.zip(maybePairs.map(_.map(p => column.unsafeDecodeValue(p._2))))
                  }
              )
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
  )(implicit transformer: DbKeyTransformer[Out, K]): Flow[In, (In, Option[V]), NotUsed] = {
    batchGetFlow(
      (in: In) =>
        _ => {
          val key = transformer.transform(extractor(in))
          DbKeyConstraints[K](Queue(DbKeyConstraint(Operator.EQUAL, ByteString.copyFrom(column.encodeKey(key)))))
        },
      maxBatchSize,
      groupWithin,
      useCache
    )
  }

  def source(implicit clientOptions: DbClientOptions): Source[(K, V), Future[NotUsed]] = {
    source(_.first, _.last)
  }

  def source(from: ConstraintsBuilder[K], to: ConstraintsBuilder[K])(
    implicit clientOptions: DbClientOptions
  ): Source[(K, V), Future[NotUsed]] = {
    val range = DbKeyConstraints.range[K](from, to)
    db.iterateSource(column, range)
      .mapAsync(1) { batch =>
        Future {
          batch.map[(K, V), List[(K, V)]](column.unsafeDecode)(breakOut)
        }(akkaEnv.dispatcher)
      }
      .mapConcat(identity)
  }

  def valueSource(from: ConstraintsBuilder[K], to: ConstraintsBuilder[K])(
    implicit clientOptions: DbClientOptions
  ): Source[V, Future[NotUsed]] = {
    val range = DbKeyConstraints.range[K](from, to)
    db.iterateValuesSource(column, range)
      .mapAsync(1) { batch =>
        Future {
          batch.map[V, List[V]](column.unsafeDecodeValue)(breakOut)
        }(akkaEnv.dispatcher)
      }
      .mapConcat(identity)
  }

  def valueSource(implicit clientOptions: DbClientOptions): Source[V, Future[NotUsed]] = {
    valueSource(_.first, _.last)
  }

  def putTask(key: K, value: V): Task[Unit] = {
    db.putTask(column, column.encodeKey(key), column.encodeValue(value))
  }

  def putValueTask(value: V)(implicit kt: DbKeyTransformer[V, K]): Task[Unit] = {
    db.putTask(column, column.encodeKey(kt.transform(value)), column.encodeValue(value))
  }

  def deleteTask(key: K): Task[Unit] = {
    db.deleteTask(column, column.encodeKey(key))
  }

  def transformValueToPairFlow(implicit kt: DbKeyTransformer[V, K]): Flow[V, (K, V), NotUsed] = {
    Flow[V]
      .map(v => (kt.transform(v), v))
  }

  private def encodeAsPutRequest(pair: (K, V)): DbTransactionAction = {
    DbTransactionAction(
      DbTransactionAction.Action.Put(
        DbPutRequest(
          columnId = column.id,
          key = ByteString.copyFrom(column.encodeKey(pair._1)),
          value = ByteString.copyFrom(column.encodeValue(pair._2))
        )
      )
    )
  }

  def putInBatchesFlow(
    maxBatchSize: Int = 4096,
    groupWithin: FiniteDuration = Duration.Zero,
    batchEncodingParallelism: Int = 2
  ): Flow[(K, V), Seq[(K, V)], NotUsed] = {
    Flow[(K, V)]
      .via(AkkaStreamUtils.batchFlow(maxBatchSize, groupWithin))
      .mapAsync(batchEncodingParallelism) { batch =>
        Future {
          batch
            .map(encodeAsPutRequest)
        }.map(encoded => (batch, encoded))
      }
      .mapAsync(1) {
        case (batch, encoded) =>
          unsafeRunToFuture(
            db.transactionTask(encoded)
              .map(_ => batch)
          )
      }
  }

  def putValuesInBatchesFlow(
    maxBatchSize: Int = 4096,
    groupWithin: FiniteDuration = Duration.Zero,
    batchEncodingParallelism: Int = 2
  )(implicit kt: DbKeyTransformer[V, K]): Flow[V, Seq[(K, V)], NotUsed] = {
    Flow[V]
      .via(transformValueToPairFlow)
      .via(putInBatchesFlow(maxBatchSize, groupWithin, batchEncodingParallelism))
  }

  def putInBatchesWithCheckpointFlow[CheckpointCol[A, B] <: DbDef#BaseCol[A, B], CK, CV](
    checkpointColumn: DbDef#Columns => CheckpointCol[CK, CV],
    maxBatchSize: Int = 4096,
    groupWithin: FiniteDuration = Duration.Zero,
    batchEncodingParallelism: Int = 1
  )(
    aggregateCheckpoint: Seq[(K, V)] => (CK, CV)
  ): Flow[(K, V), Seq[(K, V)], NotUsed] = {
    putInBatchesWithCheckpointsFlow(checkpointColumn, maxBatchSize, groupWithin, batchEncodingParallelism) { seq =>
      Vector(aggregateCheckpoint(seq))
    }
  }

  def putInBatchesWithCheckpointsFlow[CheckpointCol[A, B] <: DbDef#BaseCol[A, B], CK, CV](
    checkpointColumn: DbDef#Columns => CheckpointCol[CK, CV],
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

          for ((k, v) <- batch) txn.put(column, k, v)
          for ((ck, cv) <- aggregateCheckpoints(batch)) txn.put(checkpointColumn(db.definition.columns), ck, cv)

          txn.result
        }.map(encoded => (batch, encoded))
      }
      .mapAsync(1) {
        case (batch, encoded) =>
          unsafeRunToFuture(
            db.transactionTask(encoded)
              .map(_ => batch)
          )
      }
  }

  def putValuesInBatchesWithCheckpointFlow[CheckpointCol[A, B] <: DbDef#BaseCol[A, B], CK, CV](
    checkpointColumn: DbDef#Columns => CheckpointCol[CK, CV],
    maxBatchSize: Int = 4096,
    groupWithin: FiniteDuration = Duration.Zero,
    batchEncodingParallelism: Int = 1
  )(
    aggregateCheckpoint: Seq[(K, V)] => (CK, CV)
  )(implicit kt: DbKeyTransformer[V, K]): Flow[V, Seq[(K, V)], NotUsed] = {
    Flow[V]
      .via(transformValueToPairFlow)
      .via(
        putInBatchesWithCheckpointFlow(checkpointColumn, maxBatchSize, groupWithin, batchEncodingParallelism)(
          aggregateCheckpoint
        )
      )
  }

  def putValuesInBatchesWithCheckpointsFlow[CheckpointCol[A, B] <: DbDef#BaseCol[A, B], CK, CV](
    checkpointColumn: DbDef#Columns => CheckpointCol[CK, CV],
    maxBatchSize: Int = 4096,
    groupWithin: FiniteDuration = Duration.Zero,
    batchEncodingParallelism: Int = 1
  )(
    aggregateCheckpoints: Seq[(K, V)] => Seq[(CK, CV)]
  )(implicit kt: DbKeyTransformer[V, K]): Flow[V, Seq[(K, V)], NotUsed] = {
    Flow[V]
      .via(transformValueToPairFlow)
      .via(
        putInBatchesWithCheckpointsFlow(checkpointColumn, maxBatchSize, groupWithin, batchEncodingParallelism)(
          aggregateCheckpoints
        )
      )
  }

  def tailValueSource(
    from: ConstraintsBuilder[K],
    to: ConstraintsBuilder[K]
  )(implicit clientOptions: DbClientOptions): Source[V, Future[NotUsed]] = {
    db.tailValuesSource(column, DbKeyConstraints.range[K](from, to))
      .collect {
        case Right(b) => b
      }
      .mapAsync(1) { b =>
        Future {
          b.map[V, List[V]](column.unsafeDecodeValue)(breakOut)
        }
      }
      .mapConcat(identity)
  }

  def tailValueSource(implicit clientOptions: DbClientOptions): Source[V, Future[NotUsed]] = {
    tailValueSource(_.first, _.last)
  }

  def tailSource(
    from: ConstraintsBuilder[K],
    to: ConstraintsBuilder[K]
  )(implicit clientOptions: DbClientOptions): Source[(K, V), Future[NotUsed]] = {
    db.tailSource(column, DbKeyConstraints.range[K](from, to))
      .collect {
        case Right(b) => b
      }
      .mapAsync(1) { batch =>
        Future {
          batch.map[(K, V), List[(K, V)]](column.unsafeDecode)(breakOut)
        }
      }
      .mapConcat(identity)
  }

  def tailSource(implicit clientOptions: DbClientOptions): Source[(K, V), Future[NotUsed]] = {
    tailSource(_.first, _.last)
  }

  def tailVerboseSource(
    from: ConstraintsBuilder[K],
    to: ConstraintsBuilder[K]
  )(implicit clientOptions: DbClientOptions): Source[Either[Instant, (K, V)], Future[NotUsed]] = {
    import cats.syntax.either._
    db.tailSource(column, DbKeyConstraints.range[K](from, to))
      .mapAsync(1) {
        case Left(e) => Future.successful(List(Either.left(e.time)))
        case Right(batch) =>
          Future {
            batch.map[Either[Instant, (K, V)], List[Either[Instant, (K, V)]]](
              p => Either.right(column.unsafeDecode(p))
            )(
              breakOut
            )
          }
      }
      .mapConcat(identity)
  }

  def batchTailVerboseRawSource(
    ranges: ConstraintsRangesBuilder[K]
  )(implicit clientOptions: DbClientOptions): Source[(Int, DbTailBatch), Future[NotUsed]] = {
    val builtRanges = ranges(DbKeyConstraints.seed[K]).map {
      case (from, to) =>
        DbKeyConstraints.toRange[K](from, to)
    }

    db.batchTailSource(column, builtRanges)
  }

  def batchTailVerboseSource(
    ranges: ConstraintsRangesBuilder[K],
    decodingParallelism: Int = 1
  )(
    implicit clientOptions: DbClientOptions
  ): Source[(Int, Either[Instant, List[(K, V)]]), Future[NotUsed]] = {
    import cats.syntax.either._

    batchTailVerboseRawSource(ranges)
      .mapAsync(decodingParallelism) {
        case (index, Left(e)) => Future.successful((index, Either.left(e.time)))
        case (index, Right(batch)) =>
          Future {
            val r = Either.right[Instant, List[(K, V)]](batch.map { p =>
              column.unsafeDecode(p)
            }(breakOut))
            (index, r)
          }
      }
  }

  def tailVerboseValueSource(
    from: ConstraintsBuilder[K],
    to: ConstraintsBuilder[K]
  )(implicit clientOptions: DbClientOptions): Source[Either[Instant, V], Future[NotUsed]] = {
    import cats.syntax.either._
    db.tailValuesSource(column, DbKeyConstraints.range[K](from, to))
      .mapAsync(1) {
        case Left(e) => Future.successful(List(Either.left(e.time)))
        case Right(batch) =>
          Future {
            batch.map[Either[Instant, V], List[Either[Instant, V]]](p => Either.right(column.unsafeDecodeValue(p)))(
              breakOut
            )
          }
      }
      .mapConcat(identity)
  }

  def drop(): Task[Unit] = {
    db.dropColumnFamily(column)
  }
}
