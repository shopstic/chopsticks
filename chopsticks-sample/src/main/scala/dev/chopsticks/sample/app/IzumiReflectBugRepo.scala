package dev.chopsticks.sample.app

import izumi.reflect.Tag

object IzumiReflectBugRepoHelper {
  trait Service[Param] {}

  final case class Foo()

  val tag1 = implicitly[Tag[Service[Foo]]]
}

object IzumiReflectBugRepo {
  import IzumiReflectBugRepoHelper._

  def main(args: Array[String]): Unit = {
    val tag2 = implicitly[Tag[Service[Foo]]]

    assert(tag1.tag == tag2.tag)
  }
}
