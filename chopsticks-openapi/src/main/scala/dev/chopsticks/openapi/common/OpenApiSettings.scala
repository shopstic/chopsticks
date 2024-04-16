package dev.chopsticks.openapi.common

import zio.Chunk

trait OpenApiSettings {
  def getTypeName(packageName: Chunk[String], objectNames: Chunk[String], typeName: String): String
}
object OpenApiSettings {
  val default = new OpenApiSettings {
    override def getTypeName(packageName: Chunk[String], objectNames: Chunk[String], typeName: String): String = {
      val n1 = objectNames.mkString("_")
      if (n1.isEmpty) typeName
      else s"${n1}_$typeName"
    }
  }
}
