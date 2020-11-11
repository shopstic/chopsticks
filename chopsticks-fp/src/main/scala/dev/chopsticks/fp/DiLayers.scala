package dev.chopsticks.fp

import distage.{HasConstructor, ModuleDef}
import izumi.distage.model.definition.Module
import zio.{Has, RLayer, RManaged, Tag, ZIO}

object DiLayers {
  def apply(layers: LayerBinding*): Module = layers.foldLeft(Module.empty)(_ ++ _.layerModule)

  def narrow[R: HasConstructor, A: zio.Tag](layer: RLayer[R, Has[A]]): LayerBindingNarrow[R, A] = {
    LayerBindingNarrow[R, A](layer)
  }

  final case class LayerBinding(layerModule: ModuleDef)

  final case class LayerBindingNarrow[R: HasConstructor, A: zio.Tag](layer: RLayer[R, Has[A]]) {
    @SuppressWarnings(Array("org.wartremover.warts.NonUnitStatements"))
    def to[B >: A: zio.Tag]: LayerBinding = {
      LayerBinding(new ModuleDef {
        make[A].fromHas(layer = layer)
        make[B].using[A]
      })
    }
  }

  object LayerBinding {
    import scala.language.implicitConversions
    @SuppressWarnings(Array("org.wartremover.warts.NonUnitStatements"))
    implicit def fromLayer[R: HasConstructor, A: zio.Tag](layer: RLayer[R, Has[A]]): LayerBinding =
      LayerBinding(new ModuleDef {
        make[A].fromHas(layer = layer)
      })

    @SuppressWarnings(Array("org.wartremover.warts.NonUnitStatements"))
    implicit def fromManaged[R: HasConstructor, A: Tag](managed: RManaged[R, A]): LayerBinding =
      LayerBinding(new ModuleDef {
        make[A].fromHas(resource = managed)
      })

    @SuppressWarnings(Array("org.wartremover.warts.NonUnitStatements"))
    implicit def fromEffect[R: HasConstructor, E: Tag, A: Tag](effect: ZIO[R, E, A]): LayerBinding =
      LayerBinding(new ModuleDef {
        make[A].fromHas(effect = effect)
      })
  }
}
