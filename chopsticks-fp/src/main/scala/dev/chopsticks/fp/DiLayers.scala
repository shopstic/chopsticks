package dev.chopsticks.fp

import distage.{HasConstructor, ModuleDef, Tag}
import zio.{Has, RLayer, ZIO}
import izumi.distage.model.definition.Module

object DiLayers {
  def apply(layers: LayerBinding*): Module = layers.foldLeft(Module.empty)(_ ++ _.layerModule)

  final case class LayerBinding(layerModule: ModuleDef)
  object LayerBinding {
    import scala.language.implicitConversions
    @SuppressWarnings(Array("org.wartremover.warts.NonUnitStatements"))
    implicit def fromLayer[R: HasConstructor, A: Tag: zio.Tagged](layer: RLayer[R, Has[A]]): LayerBinding =
      LayerBinding(new ModuleDef {
        make[A].fromHas(layer = layer)
      })

    @SuppressWarnings(Array("org.wartremover.warts.NonUnitStatements"))
    implicit def fromEffect[R: HasConstructor, E: Tag, A: Tag: zio.Tagged](effect: ZIO[R, E, A]): LayerBinding =
      LayerBinding(new ModuleDef {
        make[A].fromHas(effect = effect)
      })
  }
}
