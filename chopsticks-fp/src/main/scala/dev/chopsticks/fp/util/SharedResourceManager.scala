package dev.chopsticks.fp.util

import zio._
import zio.stm.{STM, TMap}

trait SharedResourceManager[-R, Id, Res] {
  def activeSet: UIO[Set[Res]]
  def manage(id: Id): ZIO[R with Scope, Nothing, Res]
}
object SharedResourceManager {
  sealed trait BusyState
  private case object BusyCreatingState extends BusyState
  private case object BusyDestroyingState extends BusyState

  private val BusyCreating = Left(BusyCreatingState)
  private val BusyDestroying = Left(BusyDestroyingState)

  final private case class SharedResource[Res](resource: Res, acquisitionCount: Int, release: UIO[Unit])

  def fromFactory[R: zio.Tag, Id: zio.Tag, Res: zio.Tag](
    factory: ULayer[SharedResourceFactory[R, Id, Res]]
  ): TaskLayer[SharedResourceManager[R, Id, Res]] = {
    factory >>> live[R, Id, Res]
  }

  def live[R: zio.Tag, Id: zio.Tag, Res: zio.Tag]
    : URLayer[SharedResourceFactory[R, Id, Res], SharedResourceManager[R, Id, Res]] = {
    val managedTmap: URIO[Scope, TMap[Id, Either[BusyState, SharedResource[Res]]]] = ZIO
      .acquireRelease {
        TMap.empty[Id, Either[BusyState, SharedResource[Res]]].commit
      } { tmap =>
        for {
          resources <- tmap.toMap.flatMap { map =>
            STM.foreachDiscard(map.keys) { k =>
              tmap.delete(k)
            } *> STM.succeed(map.values.collect {
              case Right(r) =>
                r
            }.toList)
          }.commit
          _ <- ZIO.foreachDiscard(resources)(_.release)
        } yield tmap
      }

    val managed: ZIO[SharedResourceFactory[R, Id, Res] with Scope, Nothing, SharedResourceManager[R, Id, Res]] = for {
      tmap <- managedTmap
      factory <- ZIO.service[SharedResourceFactory[R, Id, Res]]
    } yield {
      new SharedResourceManager[R, Id, Res] {

        override def activeSet: UIO[Set[Res]] = {
          tmap.values
            .map {
              _.collect {
                case Right(res) => res.resource
              }.toSet
            }
            .commit
        }

        override def manage(id: Id): ZIO[R with Scope, Nothing, Res] = {
          ZIO.acquireRelease {
            for {
              maybeResource <- tmap.get(id)
                .flatMap {
                  case Some(Left(_)) =>
                    STM.retry
                  case Some(Right(r)) =>
                    tmap.put(id, Right(r.copy(acquisitionCount = r.acquisitionCount + 1))) *> STM.succeed(
                      Some(r.resource)
                    )
                  case None =>
                    tmap.put(id, BusyCreating) *> STM.succeed(None)
                }
                .commit
              resource <- maybeResource match {
                case Some(r) =>
                  ZIO.succeed(r)

                case None =>
                  for {
                    closeableScope <- Scope.makeWith(ExecutionStrategy.Sequential)
                    res <- factory.manage(id).provideSomeLayer[R](ZLayer.succeed(closeableScope))
                    _ <- tmap
                      .put(id, Right(SharedResource(res, 1, closeableScope.close(zio.Exit.unit))))
                      .commit
                  } yield res
              }
            } yield resource
          } { _ =>
            for {
              maybeRelease <- tmap.get(id)
                .flatMap {
                  case Some(Right(r)) =>
                    if (r.acquisitionCount == 1) {
                      tmap.put(id, BusyDestroying) *> STM.succeed(Some(r.release))
                    }
                    else {
                      tmap.put(id, Right(r.copy(acquisitionCount = r.acquisitionCount - 1))) *> STM.succeed(None)
                    }
                  case Some(Left(_)) =>
                    STM.retry
                  case None =>
                    STM.succeed(None)
                }
                .commit
              _ <- maybeRelease match {
                case Some(release) => release *> tmap.delete(id).commit
                case None => ZIO.unit
              }
            } yield ()
          }
        }
      }
    }

    ZLayer.fromZIO(ZIO.scoped(managed))
  }
}
