package com.shopstic.testkit

import akka.testkit.TestKit
import org.scalatest.{BeforeAndAfterAll, Suite}

trait AkkaTestKitAutoShutDown extends BeforeAndAfterAll {
  this: AkkaTestKit with Suite =>

  override val invokeBeforeAllAndAfterAllEvenIfNoTestsAreExpected = true

  override protected def afterAll(): Unit = {
    super.afterAll()
    TestKit.shutdownActorSystem(system, verifySystemShutdown = true)
  }
}
