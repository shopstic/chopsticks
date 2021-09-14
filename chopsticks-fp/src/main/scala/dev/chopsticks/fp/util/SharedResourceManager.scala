package dev.chopsticks.fp.util

import zio._
import zio.stm.{STM, TMap}

object SharedResourceManager {
  trait Service[-R, Id, Res] {
    def activeSet: UIO[Set[Res]]
    def manage(id: Id): ZManaged[R, Nothing, Res]
  }

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
    : RLayer[SharedResourceFactory[R, Id, Res], SharedResourceManager[R, Id, Res]] = {
    val managedTmap: UManaged[TMap[Id, Either[BusyState, SharedResource[Res]]]] = ZManaged
      .make {
        TMap.empty[Id, Either[BusyState, SharedResource[Res]]].commit
      } { tmap =>
        for {
          resources <- tmap.toMap.flatMap { map =>
            STM.foreach_(map.keys) { k =>
              tmap.delete(k)
            } *> STM.succeed(map.values.collect {
              case Right(r) =>
                r
            }.toList)
          }.commit
          _ <- ZIO.foreach_(resources)(_.release)
        } yield tmap
      }

    val managed: ZManaged[SharedResourceFactory[R, Id, Res], Nothing, Service[R, Id, Res]] = for {
      tmap <- managedTmap
      factory <- ZManaged.access[SharedResourceFactory[R, Id, Res]](_.get)
    } yield {
      new Service[R, Id, Res] {

        override def activeSet: UIO[Set[Res]] = {
          tmap.values
            .map {
              _.collect {
                case Right(res) => res.resource
              }.toSet
            }
            .commit
        }

        override def manage(id: Id): ZManaged[R, Nothing, Res] = {
          ZManaged.make {
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
                    relMap <- ZManaged.ReleaseMap.make
                    res <- factory.manage(id).zio.provideSome[R]((_, relMap)).map(_._2)
                    _ <- tmap.put(
                      id,
                      Right(SharedResource(
                        res,
                        1,
                        relMap.releaseAll(zio.Exit.Success(()), ExecutionStrategy.Sequential).unit
                      ))
                    ).commit
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

    managed.toLayer
  }
}
