package dev.chopsticks.testkit

import akka.actor.ActorSystem
import com.typesafe.config.{Config, ConfigFactory}
import dev.chopsticks.fp.{AkkaDiApp, DiLayers}
import dev.chopsticks.fp.DiEnv.LiveDiEnv
import dev.chopsticks.fp.iz_logging.IzLogging
import izumi.distage.constructors.HasConstructor
import izumi.distage.model.definition.Module
import pureconfig.{KebabCase, PascalCase}
import zio.test.{DefaultRunnableSpec, TestResult, ZSpec}
import zio.{Has, RIO, Task, URLayer, ZIO, ZLayer}

object TestLayer {
  type TestEnv = Has[Task[TestResult]]

  def apply[R](rio: RIO[R, TestResult]): URLayer[R, TestEnv] = {
    ZLayer.requires[R].map { env =>
      Has(rio.provide(env))
    }
  }
}

abstract class AkkaDiRunnableSpec extends DefaultRunnableSpec {

  protected def testEnv(config: Config): Task[Module]

  protected def loadConfig: Config = {
    val cfg = ConfigFactory.load()
    if (!cfg.getBoolean("akka.stream.materializer.debug.fuzzing-mode")) {
      throw new IllegalArgumentException(
        "akka.stream.materializer.debug.fuzzing-mode is not 'on' for testing, config loading is not working properly?"
      )
    }
    cfg
  }

  def diTestM[R: HasConstructor](label: String)(assertion: => RIO[R, TestResult]): ZSpec[Any, Throwable] = {
    testM(label)(runTest(assertion))
  }

  protected def runTest[R: HasConstructor](assertion: => RIO[R, TestResult]): Task[TestResult] = {
    for {
      config <- Task(loadConfig)
      extraModule <- testEnv(config)
      liveModule = AkkaDiApp.Env.createModule(createActorSystem(config))
      baseModule = DiLayers(
        IzLogging.live(config),
        TestLayer(assertion),
        ZIO.environment[Has[Task[TestResult]]]
      )
      liveDiEnv = LiveDiEnv[Has[Task[TestResult]]](liveModule ++ baseModule ++ extraModule)
      result <- liveDiEnv.build().use(_.get)
    } yield result
  }

  protected def createActorSystem(config: Config): ActorSystem = {
    ActorSystem(
      getClass.getName
        .filter(_.isLetterOrDigit).split("\\.")
        .map(n => KebabCase.fromTokens(PascalCase.toTokens(n)))
        .mkString("-"),
      config
    )
  }
}
