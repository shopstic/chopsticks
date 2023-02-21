package dev.chopsticks.schema

import zio.Chunk

import scala.deriving.Mirror
import scala.Tuple.Union

enum SchemaSumTypeSerDeStrategy[A]:
  case Discriminator(discriminator: SchemaDiscriminator[A])

trait SchemaDiscriminator[A]:
  private type DiscriminatorValue = String
  private type SubTypeName = String
  def discriminatorFieldName: String
  def mapping: Map[DiscriminatorValue, SubTypeName]
  def discriminatorValue(obj: A): DiscriminatorValue

object SchemaDiscriminator:

  final class SchemaDiscriminatorDerived[A, AnyLabel](
    override val discriminatorFieldName: String,
    subtypes: Chunk[AnyLabel],
    extractDiscriminatorValue: A => AnyLabel
  ) extends SchemaDiscriminator[A]:
    self =>
    override def discriminatorValue(obj: A): String =
      extractDiscriminatorValue(obj).asInstanceOf[String]
    override val mapping: Map[String, String] =
      subtypes.iterator.map(s => s.asInstanceOf[String] -> s.asInstanceOf[String]).toMap
    def transformSubTypeNames(f: AnyLabel => String)(using AnyLabel <:< String): SchemaDiscriminator[A] =
      new SchemaDiscriminator[A]:
        override val discriminatorFieldName = self.discriminatorFieldName
        override val mapping = subtypes.iterator.map { s => f(s) -> s.asInstanceOf[String] }.toMap
        override def discriminatorValue(obj: A): String =
          f(self.extractDiscriminatorValue(obj))
  end SchemaDiscriminatorDerived

  inline def derive[A](using
    m: Mirror.SumOf[A]
  ): SchemaDiscriminatorDerived[A, Union[m.MirroredElemLabels]] =
    derive[A](fieldName = "type")

  inline def derive[A](fieldName: String)(using
    m: Mirror.SumOf[A]
  ): SchemaDiscriminatorDerived[A, Union[m.MirroredElemLabels]] =
    val elemLabels: Chunk[Union[m.MirroredElemLabels]] =
      Chunk
        .fromIterable(scala.compiletime.constValueTuple[m.MirroredElemLabels].toList)
        .asInstanceOf[Chunk[Union[m.MirroredElemLabels]]]
    new SchemaDiscriminatorDerived[A, Union[m.MirroredElemLabels]](
      fieldName,
      // cast below is really needed, otherwise it fails on the caller side
      elemLabels.asInstanceOf[Chunk[Union[m.MirroredElemLabels]]],
      extractDiscriminatorValue = (obj: A) => elemLabels(m.ordinal(obj))
    )
