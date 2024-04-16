package dev.chopsticks.openapi.common

import dev.chopsticks.openapi.OpenApiParsedAnnotations
import zio.schema.TypeId

final private[chopsticks] case class ConverterCacheKey(entityName: String, annotationsHash: Int)

final private[chopsticks] class ConverterCache[C[_]](
  cache: scala.collection.mutable.Map[ConverterCacheKey, C[_] with ConverterCache.Lazy[C[_]]] =
    scala.collection.mutable.Map.empty[ConverterCacheKey, C[_] with ConverterCache.Lazy[C[_]]]
) {
  private[chopsticks] def convertUsingCache[A](
    typeId: TypeId,
    annotations: OpenApiParsedAnnotations[A]
  )(convert: => C[A])(
    initLazy: () => C[A] with ConverterCache.Lazy[C[A]]
  ): C[A] = {
    val entityName = OpenApiConverterUtils.getEntityName(Some(typeId), annotations)
    entityName match {
      case Some(name) =>
        val cacheKey = ConverterCacheKey(name, annotations.hashCode())
        cache.get(cacheKey) match {
          case Some(value) => value.asInstanceOf[C[A]]
          case None =>
            val lazyEnc = initLazy()
            val _ = cache.addOne(cacheKey -> lazyEnc.asInstanceOf[C[_] with ConverterCache.Lazy[C[_]]])
            val result = convert
            lazyEnc.set(result)
            result
        }
      case None =>
        convert
    }
  }
}
object ConverterCache {
  private[chopsticks] trait Lazy[A] {
    private var _value: A = _
    final private[ConverterCache] def set(value: A): Unit = _value = value
    final private[chopsticks] def get: A =
      if (_value == null) throw new RuntimeException(s"${this.getClass.getSimpleName} has not yet been initialized")
      else _value
  }
}
