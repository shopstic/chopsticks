package dev.chopsticks.csv

import scala.collection.immutable.ArraySeq
import scala.collection.mutable.ListBuffer

trait CsvProductDecoders {
  // scalafmt: { maxColumn = 800, optIn.configStyleArguments = false }

  final def forProduct1[Target, A0](
    options: CsvCodecOptions,
    names: ArraySeq[String],
    decoders: ArraySeq[CsvDecoder[_]]
  )(construct: scala.Function1[A0, Target]): CsvDecoder[Target] = {
    new CsvDecoder[Target] {
      override val isOptional = false
      override val isPrimitive = false
      override def parseAsOption(row: Map[String, String], columnName: Option[String]): Either[List[CsvDecoderError], Option[Target]] = {
        parseProductAsOption(names, decoders, row, columnName, options) match {
          case Left(errors) => Left(errors)
          case Right(None) => Right(None)
          case Right(Some(results)) =>
            Right(
              Some(
                construct(
                  results(0).asInstanceOf[A0]
                )
              )
            )
        }
      }

      override def parse(row: Map[String, String], columnName: Option[String]) = {
        val errors = new ListBuffer[CsvDecoderError]
        val results = new Array[Any](names.length)
        var i = 0
        while (i < names.length) {
          val decoder = decoders(i)
          val fieldName = names(i)
          decoder.parse(row, Some(options.nestedFieldLabel(columnName, fieldName))) match {
            case Right(value) => val _ = results(i) = value
            case Left(errs) => val _ = errors.addAll(errs)
          }
          i += 1
        }
        if (errors.nonEmpty) Left(errors.toList)
        else {
          Right(
            construct(
              results(0).asInstanceOf[A0]
            )
          )
        }
      }
    }
  }

  final def forProduct2[Target, A0, A1](
    options: CsvCodecOptions,
    names: ArraySeq[String],
    decoders: ArraySeq[CsvDecoder[_]]
  )(construct: scala.Function2[A0, A1, Target]): CsvDecoder[Target] = {
    new CsvDecoder[Target] {
      override val isOptional = false
      override val isPrimitive = false
      override def parseAsOption(row: Map[String, String], columnName: Option[String]): Either[List[CsvDecoderError], Option[Target]] = {
        parseProductAsOption(names, decoders, row, columnName, options) match {
          case Left(errors) => Left(errors)
          case Right(None) => Right(None)
          case Right(Some(results)) =>
            Right(
              Some(
                construct(
                  results(0).asInstanceOf[A0],
                  results(1).asInstanceOf[A1]
                )
              )
            )
        }
      }

      override def parse(row: Map[String, String], columnName: Option[String]) = {
        val errors = new ListBuffer[CsvDecoderError]
        val results = new Array[Any](names.length)
        var i = 0
        while (i < names.length) {
          val decoder = decoders(i)
          val fieldName = names(i)
          decoder.parse(row, Some(options.nestedFieldLabel(columnName, fieldName))) match {
            case Right(value) => val _ = results(i) = value
            case Left(errs) => val _ = errors.addAll(errs)
          }
          i += 1
        }
        if (errors.nonEmpty) Left(errors.toList)
        else {
          Right(
            construct(
              results(0).asInstanceOf[A0],
              results(1).asInstanceOf[A1]
            )
          )
        }
      }
    }
  }

  final def forProduct3[Target, A0, A1, A2](
    options: CsvCodecOptions,
    names: ArraySeq[String],
    decoders: ArraySeq[CsvDecoder[_]]
  )(construct: scala.Function3[A0, A1, A2, Target]): CsvDecoder[Target] = {
    new CsvDecoder[Target] {
      override val isOptional = false
      override val isPrimitive = false
      override def parseAsOption(row: Map[String, String], columnName: Option[String]): Either[List[CsvDecoderError], Option[Target]] = {
        parseProductAsOption(names, decoders, row, columnName, options) match {
          case Left(errors) => Left(errors)
          case Right(None) => Right(None)
          case Right(Some(results)) =>
            Right(
              Some(
                construct(
                  results(0).asInstanceOf[A0],
                  results(1).asInstanceOf[A1],
                  results(2).asInstanceOf[A2]
                )
              )
            )
        }
      }

      override def parse(row: Map[String, String], columnName: Option[String]) = {
        val errors = new ListBuffer[CsvDecoderError]
        val results = new Array[Any](names.length)
        var i = 0
        while (i < names.length) {
          val decoder = decoders(i)
          val fieldName = names(i)
          decoder.parse(row, Some(options.nestedFieldLabel(columnName, fieldName))) match {
            case Right(value) => val _ = results(i) = value
            case Left(errs) => val _ = errors.addAll(errs)
          }
          i += 1
        }
        if (errors.nonEmpty) Left(errors.toList)
        else {
          Right(
            construct(
              results(0).asInstanceOf[A0],
              results(1).asInstanceOf[A1],
              results(2).asInstanceOf[A2]
            )
          )
        }
      }
    }
  }

  final def forProduct4[Target, A0, A1, A2, A3](
    options: CsvCodecOptions,
    names: ArraySeq[String],
    decoders: ArraySeq[CsvDecoder[_]]
  )(construct: scala.Function4[A0, A1, A2, A3, Target]): CsvDecoder[Target] = {
    new CsvDecoder[Target] {
      override val isOptional = false
      override val isPrimitive = false
      override def parseAsOption(row: Map[String, String], columnName: Option[String]): Either[List[CsvDecoderError], Option[Target]] = {
        parseProductAsOption(names, decoders, row, columnName, options) match {
          case Left(errors) => Left(errors)
          case Right(None) => Right(None)
          case Right(Some(results)) =>
            Right(
              Some(
                construct(
                  results(0).asInstanceOf[A0],
                  results(1).asInstanceOf[A1],
                  results(2).asInstanceOf[A2],
                  results(3).asInstanceOf[A3]
                )
              )
            )
        }
      }

      override def parse(row: Map[String, String], columnName: Option[String]) = {
        val errors = new ListBuffer[CsvDecoderError]
        val results = new Array[Any](names.length)
        var i = 0
        while (i < names.length) {
          val decoder = decoders(i)
          val fieldName = names(i)
          decoder.parse(row, Some(options.nestedFieldLabel(columnName, fieldName))) match {
            case Right(value) => val _ = results(i) = value
            case Left(errs) => val _ = errors.addAll(errs)
          }
          i += 1
        }
        if (errors.nonEmpty) Left(errors.toList)
        else {
          Right(
            construct(
              results(0).asInstanceOf[A0],
              results(1).asInstanceOf[A1],
              results(2).asInstanceOf[A2],
              results(3).asInstanceOf[A3]
            )
          )
        }
      }
    }
  }

  final def forProduct5[Target, A0, A1, A2, A3, A4](
    options: CsvCodecOptions,
    names: ArraySeq[String],
    decoders: ArraySeq[CsvDecoder[_]]
  )(construct: scala.Function5[A0, A1, A2, A3, A4, Target]): CsvDecoder[Target] = {
    new CsvDecoder[Target] {
      override val isOptional = false
      override val isPrimitive = false
      override def parseAsOption(row: Map[String, String], columnName: Option[String]): Either[List[CsvDecoderError], Option[Target]] = {
        parseProductAsOption(names, decoders, row, columnName, options) match {
          case Left(errors) => Left(errors)
          case Right(None) => Right(None)
          case Right(Some(results)) =>
            Right(
              Some(
                construct(
                  results(0).asInstanceOf[A0],
                  results(1).asInstanceOf[A1],
                  results(2).asInstanceOf[A2],
                  results(3).asInstanceOf[A3],
                  results(4).asInstanceOf[A4]
                )
              )
            )
        }
      }

      override def parse(row: Map[String, String], columnName: Option[String]) = {
        val errors = new ListBuffer[CsvDecoderError]
        val results = new Array[Any](names.length)
        var i = 0
        while (i < names.length) {
          val decoder = decoders(i)
          val fieldName = names(i)
          decoder.parse(row, Some(options.nestedFieldLabel(columnName, fieldName))) match {
            case Right(value) => val _ = results(i) = value
            case Left(errs) => val _ = errors.addAll(errs)
          }
          i += 1
        }
        if (errors.nonEmpty) Left(errors.toList)
        else {
          Right(
            construct(
              results(0).asInstanceOf[A0],
              results(1).asInstanceOf[A1],
              results(2).asInstanceOf[A2],
              results(3).asInstanceOf[A3],
              results(4).asInstanceOf[A4]
            )
          )
        }
      }
    }
  }

  final def forProduct6[Target, A0, A1, A2, A3, A4, A5](
    options: CsvCodecOptions,
    names: ArraySeq[String],
    decoders: ArraySeq[CsvDecoder[_]]
  )(construct: scala.Function6[A0, A1, A2, A3, A4, A5, Target]): CsvDecoder[Target] = {
    new CsvDecoder[Target] {
      override val isOptional = false
      override val isPrimitive = false
      override def parseAsOption(row: Map[String, String], columnName: Option[String]): Either[List[CsvDecoderError], Option[Target]] = {
        parseProductAsOption(names, decoders, row, columnName, options) match {
          case Left(errors) => Left(errors)
          case Right(None) => Right(None)
          case Right(Some(results)) =>
            Right(
              Some(
                construct(
                  results(0).asInstanceOf[A0],
                  results(1).asInstanceOf[A1],
                  results(2).asInstanceOf[A2],
                  results(3).asInstanceOf[A3],
                  results(4).asInstanceOf[A4],
                  results(5).asInstanceOf[A5]
                )
              )
            )
        }
      }

      override def parse(row: Map[String, String], columnName: Option[String]) = {
        val errors = new ListBuffer[CsvDecoderError]
        val results = new Array[Any](names.length)
        var i = 0
        while (i < names.length) {
          val decoder = decoders(i)
          val fieldName = names(i)
          decoder.parse(row, Some(options.nestedFieldLabel(columnName, fieldName))) match {
            case Right(value) => val _ = results(i) = value
            case Left(errs) => val _ = errors.addAll(errs)
          }
          i += 1
        }
        if (errors.nonEmpty) Left(errors.toList)
        else {
          Right(
            construct(
              results(0).asInstanceOf[A0],
              results(1).asInstanceOf[A1],
              results(2).asInstanceOf[A2],
              results(3).asInstanceOf[A3],
              results(4).asInstanceOf[A4],
              results(5).asInstanceOf[A5]
            )
          )
        }
      }
    }
  }

  final def forProduct7[Target, A0, A1, A2, A3, A4, A5, A6](
    options: CsvCodecOptions,
    names: ArraySeq[String],
    decoders: ArraySeq[CsvDecoder[_]]
  )(construct: scala.Function7[A0, A1, A2, A3, A4, A5, A6, Target]): CsvDecoder[Target] = {
    new CsvDecoder[Target] {
      override val isOptional = false
      override val isPrimitive = false
      override def parseAsOption(row: Map[String, String], columnName: Option[String]): Either[List[CsvDecoderError], Option[Target]] = {
        parseProductAsOption(names, decoders, row, columnName, options) match {
          case Left(errors) => Left(errors)
          case Right(None) => Right(None)
          case Right(Some(results)) =>
            Right(
              Some(
                construct(
                  results(0).asInstanceOf[A0],
                  results(1).asInstanceOf[A1],
                  results(2).asInstanceOf[A2],
                  results(3).asInstanceOf[A3],
                  results(4).asInstanceOf[A4],
                  results(5).asInstanceOf[A5],
                  results(6).asInstanceOf[A6]
                )
              )
            )
        }
      }

      override def parse(row: Map[String, String], columnName: Option[String]) = {
        val errors = new ListBuffer[CsvDecoderError]
        val results = new Array[Any](names.length)
        var i = 0
        while (i < names.length) {
          val decoder = decoders(i)
          val fieldName = names(i)
          decoder.parse(row, Some(options.nestedFieldLabel(columnName, fieldName))) match {
            case Right(value) => val _ = results(i) = value
            case Left(errs) => val _ = errors.addAll(errs)
          }
          i += 1
        }
        if (errors.nonEmpty) Left(errors.toList)
        else {
          Right(
            construct(
              results(0).asInstanceOf[A0],
              results(1).asInstanceOf[A1],
              results(2).asInstanceOf[A2],
              results(3).asInstanceOf[A3],
              results(4).asInstanceOf[A4],
              results(5).asInstanceOf[A5],
              results(6).asInstanceOf[A6]
            )
          )
        }
      }
    }
  }

  final def forProduct8[Target, A0, A1, A2, A3, A4, A5, A6, A7](
    options: CsvCodecOptions,
    names: ArraySeq[String],
    decoders: ArraySeq[CsvDecoder[_]]
  )(construct: scala.Function8[A0, A1, A2, A3, A4, A5, A6, A7, Target]): CsvDecoder[Target] = {
    new CsvDecoder[Target] {
      override val isOptional = false
      override val isPrimitive = false
      override def parseAsOption(row: Map[String, String], columnName: Option[String]): Either[List[CsvDecoderError], Option[Target]] = {
        parseProductAsOption(names, decoders, row, columnName, options) match {
          case Left(errors) => Left(errors)
          case Right(None) => Right(None)
          case Right(Some(results)) =>
            Right(
              Some(
                construct(
                  results(0).asInstanceOf[A0],
                  results(1).asInstanceOf[A1],
                  results(2).asInstanceOf[A2],
                  results(3).asInstanceOf[A3],
                  results(4).asInstanceOf[A4],
                  results(5).asInstanceOf[A5],
                  results(6).asInstanceOf[A6],
                  results(7).asInstanceOf[A7]
                )
              )
            )
        }
      }

      override def parse(row: Map[String, String], columnName: Option[String]) = {
        val errors = new ListBuffer[CsvDecoderError]
        val results = new Array[Any](names.length)
        var i = 0
        while (i < names.length) {
          val decoder = decoders(i)
          val fieldName = names(i)
          decoder.parse(row, Some(options.nestedFieldLabel(columnName, fieldName))) match {
            case Right(value) => val _ = results(i) = value
            case Left(errs) => val _ = errors.addAll(errs)
          }
          i += 1
        }
        if (errors.nonEmpty) Left(errors.toList)
        else {
          Right(
            construct(
              results(0).asInstanceOf[A0],
              results(1).asInstanceOf[A1],
              results(2).asInstanceOf[A2],
              results(3).asInstanceOf[A3],
              results(4).asInstanceOf[A4],
              results(5).asInstanceOf[A5],
              results(6).asInstanceOf[A6],
              results(7).asInstanceOf[A7]
            )
          )
        }
      }
    }
  }

  final def forProduct9[Target, A0, A1, A2, A3, A4, A5, A6, A7, A8](
    options: CsvCodecOptions,
    names: ArraySeq[String],
    decoders: ArraySeq[CsvDecoder[_]]
  )(construct: scala.Function9[A0, A1, A2, A3, A4, A5, A6, A7, A8, Target]): CsvDecoder[Target] = {
    new CsvDecoder[Target] {
      override val isOptional = false
      override val isPrimitive = false
      override def parseAsOption(row: Map[String, String], columnName: Option[String]): Either[List[CsvDecoderError], Option[Target]] = {
        parseProductAsOption(names, decoders, row, columnName, options) match {
          case Left(errors) => Left(errors)
          case Right(None) => Right(None)
          case Right(Some(results)) =>
            Right(
              Some(
                construct(
                  results(0).asInstanceOf[A0],
                  results(1).asInstanceOf[A1],
                  results(2).asInstanceOf[A2],
                  results(3).asInstanceOf[A3],
                  results(4).asInstanceOf[A4],
                  results(5).asInstanceOf[A5],
                  results(6).asInstanceOf[A6],
                  results(7).asInstanceOf[A7],
                  results(8).asInstanceOf[A8]
                )
              )
            )
        }
      }

      override def parse(row: Map[String, String], columnName: Option[String]) = {
        val errors = new ListBuffer[CsvDecoderError]
        val results = new Array[Any](names.length)
        var i = 0
        while (i < names.length) {
          val decoder = decoders(i)
          val fieldName = names(i)
          decoder.parse(row, Some(options.nestedFieldLabel(columnName, fieldName))) match {
            case Right(value) => val _ = results(i) = value
            case Left(errs) => val _ = errors.addAll(errs)
          }
          i += 1
        }
        if (errors.nonEmpty) Left(errors.toList)
        else {
          Right(
            construct(
              results(0).asInstanceOf[A0],
              results(1).asInstanceOf[A1],
              results(2).asInstanceOf[A2],
              results(3).asInstanceOf[A3],
              results(4).asInstanceOf[A4],
              results(5).asInstanceOf[A5],
              results(6).asInstanceOf[A6],
              results(7).asInstanceOf[A7],
              results(8).asInstanceOf[A8]
            )
          )
        }
      }
    }
  }

  final def forProduct10[Target, A0, A1, A2, A3, A4, A5, A6, A7, A8, A9](
    options: CsvCodecOptions,
    names: ArraySeq[String],
    decoders: ArraySeq[CsvDecoder[_]]
  )(construct: scala.Function10[A0, A1, A2, A3, A4, A5, A6, A7, A8, A9, Target]): CsvDecoder[Target] = {
    new CsvDecoder[Target] {
      override val isOptional = false
      override val isPrimitive = false
      override def parseAsOption(row: Map[String, String], columnName: Option[String]): Either[List[CsvDecoderError], Option[Target]] = {
        parseProductAsOption(names, decoders, row, columnName, options) match {
          case Left(errors) => Left(errors)
          case Right(None) => Right(None)
          case Right(Some(results)) =>
            Right(
              Some(
                construct(
                  results(0).asInstanceOf[A0],
                  results(1).asInstanceOf[A1],
                  results(2).asInstanceOf[A2],
                  results(3).asInstanceOf[A3],
                  results(4).asInstanceOf[A4],
                  results(5).asInstanceOf[A5],
                  results(6).asInstanceOf[A6],
                  results(7).asInstanceOf[A7],
                  results(8).asInstanceOf[A8],
                  results(9).asInstanceOf[A9]
                )
              )
            )
        }
      }

      override def parse(row: Map[String, String], columnName: Option[String]) = {
        val errors = new ListBuffer[CsvDecoderError]
        val results = new Array[Any](names.length)
        var i = 0
        while (i < names.length) {
          val decoder = decoders(i)
          val fieldName = names(i)
          decoder.parse(row, Some(options.nestedFieldLabel(columnName, fieldName))) match {
            case Right(value) => val _ = results(i) = value
            case Left(errs) => val _ = errors.addAll(errs)
          }
          i += 1
        }
        if (errors.nonEmpty) Left(errors.toList)
        else {
          Right(
            construct(
              results(0).asInstanceOf[A0],
              results(1).asInstanceOf[A1],
              results(2).asInstanceOf[A2],
              results(3).asInstanceOf[A3],
              results(4).asInstanceOf[A4],
              results(5).asInstanceOf[A5],
              results(6).asInstanceOf[A6],
              results(7).asInstanceOf[A7],
              results(8).asInstanceOf[A8],
              results(9).asInstanceOf[A9]
            )
          )
        }
      }
    }
  }

  final def forProduct11[Target, A0, A1, A2, A3, A4, A5, A6, A7, A8, A9, A10](
    options: CsvCodecOptions,
    names: ArraySeq[String],
    decoders: ArraySeq[CsvDecoder[_]]
  )(construct: scala.Function11[A0, A1, A2, A3, A4, A5, A6, A7, A8, A9, A10, Target]): CsvDecoder[Target] = {
    new CsvDecoder[Target] {
      override val isOptional = false
      override val isPrimitive = false
      override def parseAsOption(row: Map[String, String], columnName: Option[String]): Either[List[CsvDecoderError], Option[Target]] = {
        parseProductAsOption(names, decoders, row, columnName, options) match {
          case Left(errors) => Left(errors)
          case Right(None) => Right(None)
          case Right(Some(results)) =>
            Right(
              Some(
                construct(
                  results(0).asInstanceOf[A0],
                  results(1).asInstanceOf[A1],
                  results(2).asInstanceOf[A2],
                  results(3).asInstanceOf[A3],
                  results(4).asInstanceOf[A4],
                  results(5).asInstanceOf[A5],
                  results(6).asInstanceOf[A6],
                  results(7).asInstanceOf[A7],
                  results(8).asInstanceOf[A8],
                  results(9).asInstanceOf[A9],
                  results(10).asInstanceOf[A10]
                )
              )
            )
        }
      }

      override def parse(row: Map[String, String], columnName: Option[String]) = {
        val errors = new ListBuffer[CsvDecoderError]
        val results = new Array[Any](names.length)
        var i = 0
        while (i < names.length) {
          val decoder = decoders(i)
          val fieldName = names(i)
          decoder.parse(row, Some(options.nestedFieldLabel(columnName, fieldName))) match {
            case Right(value) => val _ = results(i) = value
            case Left(errs) => val _ = errors.addAll(errs)
          }
          i += 1
        }
        if (errors.nonEmpty) Left(errors.toList)
        else {
          Right(
            construct(
              results(0).asInstanceOf[A0],
              results(1).asInstanceOf[A1],
              results(2).asInstanceOf[A2],
              results(3).asInstanceOf[A3],
              results(4).asInstanceOf[A4],
              results(5).asInstanceOf[A5],
              results(6).asInstanceOf[A6],
              results(7).asInstanceOf[A7],
              results(8).asInstanceOf[A8],
              results(9).asInstanceOf[A9],
              results(10).asInstanceOf[A10]
            )
          )
        }
      }
    }
  }

  final def forProduct12[Target, A0, A1, A2, A3, A4, A5, A6, A7, A8, A9, A10, A11](
    options: CsvCodecOptions,
    names: ArraySeq[String],
    decoders: ArraySeq[CsvDecoder[_]]
  )(construct: scala.Function12[A0, A1, A2, A3, A4, A5, A6, A7, A8, A9, A10, A11, Target]): CsvDecoder[Target] = {
    new CsvDecoder[Target] {
      override val isOptional = false
      override val isPrimitive = false
      override def parseAsOption(row: Map[String, String], columnName: Option[String]): Either[List[CsvDecoderError], Option[Target]] = {
        parseProductAsOption(names, decoders, row, columnName, options) match {
          case Left(errors) => Left(errors)
          case Right(None) => Right(None)
          case Right(Some(results)) =>
            Right(
              Some(
                construct(
                  results(0).asInstanceOf[A0],
                  results(1).asInstanceOf[A1],
                  results(2).asInstanceOf[A2],
                  results(3).asInstanceOf[A3],
                  results(4).asInstanceOf[A4],
                  results(5).asInstanceOf[A5],
                  results(6).asInstanceOf[A6],
                  results(7).asInstanceOf[A7],
                  results(8).asInstanceOf[A8],
                  results(9).asInstanceOf[A9],
                  results(10).asInstanceOf[A10],
                  results(11).asInstanceOf[A11]
                )
              )
            )
        }
      }

      override def parse(row: Map[String, String], columnName: Option[String]) = {
        val errors = new ListBuffer[CsvDecoderError]
        val results = new Array[Any](names.length)
        var i = 0
        while (i < names.length) {
          val decoder = decoders(i)
          val fieldName = names(i)
          decoder.parse(row, Some(options.nestedFieldLabel(columnName, fieldName))) match {
            case Right(value) => val _ = results(i) = value
            case Left(errs) => val _ = errors.addAll(errs)
          }
          i += 1
        }
        if (errors.nonEmpty) Left(errors.toList)
        else {
          Right(
            construct(
              results(0).asInstanceOf[A0],
              results(1).asInstanceOf[A1],
              results(2).asInstanceOf[A2],
              results(3).asInstanceOf[A3],
              results(4).asInstanceOf[A4],
              results(5).asInstanceOf[A5],
              results(6).asInstanceOf[A6],
              results(7).asInstanceOf[A7],
              results(8).asInstanceOf[A8],
              results(9).asInstanceOf[A9],
              results(10).asInstanceOf[A10],
              results(11).asInstanceOf[A11]
            )
          )
        }
      }
    }
  }

  final def forProduct13[Target, A0, A1, A2, A3, A4, A5, A6, A7, A8, A9, A10, A11, A12](
    options: CsvCodecOptions,
    names: ArraySeq[String],
    decoders: ArraySeq[CsvDecoder[_]]
  )(construct: scala.Function13[A0, A1, A2, A3, A4, A5, A6, A7, A8, A9, A10, A11, A12, Target]): CsvDecoder[Target] = {
    new CsvDecoder[Target] {
      override val isOptional = false
      override val isPrimitive = false
      override def parseAsOption(row: Map[String, String], columnName: Option[String]): Either[List[CsvDecoderError], Option[Target]] = {
        parseProductAsOption(names, decoders, row, columnName, options) match {
          case Left(errors) => Left(errors)
          case Right(None) => Right(None)
          case Right(Some(results)) =>
            Right(
              Some(
                construct(
                  results(0).asInstanceOf[A0],
                  results(1).asInstanceOf[A1],
                  results(2).asInstanceOf[A2],
                  results(3).asInstanceOf[A3],
                  results(4).asInstanceOf[A4],
                  results(5).asInstanceOf[A5],
                  results(6).asInstanceOf[A6],
                  results(7).asInstanceOf[A7],
                  results(8).asInstanceOf[A8],
                  results(9).asInstanceOf[A9],
                  results(10).asInstanceOf[A10],
                  results(11).asInstanceOf[A11],
                  results(12).asInstanceOf[A12]
                )
              )
            )
        }
      }

      override def parse(row: Map[String, String], columnName: Option[String]) = {
        val errors = new ListBuffer[CsvDecoderError]
        val results = new Array[Any](names.length)
        var i = 0
        while (i < names.length) {
          val decoder = decoders(i)
          val fieldName = names(i)
          decoder.parse(row, Some(options.nestedFieldLabel(columnName, fieldName))) match {
            case Right(value) => val _ = results(i) = value
            case Left(errs) => val _ = errors.addAll(errs)
          }
          i += 1
        }
        if (errors.nonEmpty) Left(errors.toList)
        else {
          Right(
            construct(
              results(0).asInstanceOf[A0],
              results(1).asInstanceOf[A1],
              results(2).asInstanceOf[A2],
              results(3).asInstanceOf[A3],
              results(4).asInstanceOf[A4],
              results(5).asInstanceOf[A5],
              results(6).asInstanceOf[A6],
              results(7).asInstanceOf[A7],
              results(8).asInstanceOf[A8],
              results(9).asInstanceOf[A9],
              results(10).asInstanceOf[A10],
              results(11).asInstanceOf[A11],
              results(12).asInstanceOf[A12]
            )
          )
        }
      }
    }
  }

  final def forProduct14[Target, A0, A1, A2, A3, A4, A5, A6, A7, A8, A9, A10, A11, A12, A13](
    options: CsvCodecOptions,
    names: ArraySeq[String],
    decoders: ArraySeq[CsvDecoder[_]]
  )(construct: scala.Function14[A0, A1, A2, A3, A4, A5, A6, A7, A8, A9, A10, A11, A12, A13, Target]): CsvDecoder[Target] = {
    new CsvDecoder[Target] {
      override val isOptional = false
      override val isPrimitive = false
      override def parseAsOption(row: Map[String, String], columnName: Option[String]): Either[List[CsvDecoderError], Option[Target]] = {
        parseProductAsOption(names, decoders, row, columnName, options) match {
          case Left(errors) => Left(errors)
          case Right(None) => Right(None)
          case Right(Some(results)) =>
            Right(
              Some(
                construct(
                  results(0).asInstanceOf[A0],
                  results(1).asInstanceOf[A1],
                  results(2).asInstanceOf[A2],
                  results(3).asInstanceOf[A3],
                  results(4).asInstanceOf[A4],
                  results(5).asInstanceOf[A5],
                  results(6).asInstanceOf[A6],
                  results(7).asInstanceOf[A7],
                  results(8).asInstanceOf[A8],
                  results(9).asInstanceOf[A9],
                  results(10).asInstanceOf[A10],
                  results(11).asInstanceOf[A11],
                  results(12).asInstanceOf[A12],
                  results(13).asInstanceOf[A13]
                )
              )
            )
        }
      }

      override def parse(row: Map[String, String], columnName: Option[String]) = {
        val errors = new ListBuffer[CsvDecoderError]
        val results = new Array[Any](names.length)
        var i = 0
        while (i < names.length) {
          val decoder = decoders(i)
          val fieldName = names(i)
          decoder.parse(row, Some(options.nestedFieldLabel(columnName, fieldName))) match {
            case Right(value) => val _ = results(i) = value
            case Left(errs) => val _ = errors.addAll(errs)
          }
          i += 1
        }
        if (errors.nonEmpty) Left(errors.toList)
        else {
          Right(
            construct(
              results(0).asInstanceOf[A0],
              results(1).asInstanceOf[A1],
              results(2).asInstanceOf[A2],
              results(3).asInstanceOf[A3],
              results(4).asInstanceOf[A4],
              results(5).asInstanceOf[A5],
              results(6).asInstanceOf[A6],
              results(7).asInstanceOf[A7],
              results(8).asInstanceOf[A8],
              results(9).asInstanceOf[A9],
              results(10).asInstanceOf[A10],
              results(11).asInstanceOf[A11],
              results(12).asInstanceOf[A12],
              results(13).asInstanceOf[A13]
            )
          )
        }
      }
    }
  }

  final def forProduct15[Target, A0, A1, A2, A3, A4, A5, A6, A7, A8, A9, A10, A11, A12, A13, A14](
    options: CsvCodecOptions,
    names: ArraySeq[String],
    decoders: ArraySeq[CsvDecoder[_]]
  )(construct: scala.Function15[A0, A1, A2, A3, A4, A5, A6, A7, A8, A9, A10, A11, A12, A13, A14, Target]): CsvDecoder[Target] = {
    new CsvDecoder[Target] {
      override val isOptional = false
      override val isPrimitive = false
      override def parseAsOption(row: Map[String, String], columnName: Option[String]): Either[List[CsvDecoderError], Option[Target]] = {
        parseProductAsOption(names, decoders, row, columnName, options) match {
          case Left(errors) => Left(errors)
          case Right(None) => Right(None)
          case Right(Some(results)) =>
            Right(
              Some(
                construct(
                  results(0).asInstanceOf[A0],
                  results(1).asInstanceOf[A1],
                  results(2).asInstanceOf[A2],
                  results(3).asInstanceOf[A3],
                  results(4).asInstanceOf[A4],
                  results(5).asInstanceOf[A5],
                  results(6).asInstanceOf[A6],
                  results(7).asInstanceOf[A7],
                  results(8).asInstanceOf[A8],
                  results(9).asInstanceOf[A9],
                  results(10).asInstanceOf[A10],
                  results(11).asInstanceOf[A11],
                  results(12).asInstanceOf[A12],
                  results(13).asInstanceOf[A13],
                  results(14).asInstanceOf[A14]
                )
              )
            )
        }
      }

      override def parse(row: Map[String, String], columnName: Option[String]) = {
        val errors = new ListBuffer[CsvDecoderError]
        val results = new Array[Any](names.length)
        var i = 0
        while (i < names.length) {
          val decoder = decoders(i)
          val fieldName = names(i)
          decoder.parse(row, Some(options.nestedFieldLabel(columnName, fieldName))) match {
            case Right(value) => val _ = results(i) = value
            case Left(errs) => val _ = errors.addAll(errs)
          }
          i += 1
        }
        if (errors.nonEmpty) Left(errors.toList)
        else {
          Right(
            construct(
              results(0).asInstanceOf[A0],
              results(1).asInstanceOf[A1],
              results(2).asInstanceOf[A2],
              results(3).asInstanceOf[A3],
              results(4).asInstanceOf[A4],
              results(5).asInstanceOf[A5],
              results(6).asInstanceOf[A6],
              results(7).asInstanceOf[A7],
              results(8).asInstanceOf[A8],
              results(9).asInstanceOf[A9],
              results(10).asInstanceOf[A10],
              results(11).asInstanceOf[A11],
              results(12).asInstanceOf[A12],
              results(13).asInstanceOf[A13],
              results(14).asInstanceOf[A14]
            )
          )
        }
      }
    }
  }

  final def forProduct16[Target, A0, A1, A2, A3, A4, A5, A6, A7, A8, A9, A10, A11, A12, A13, A14, A15](
    options: CsvCodecOptions,
    names: ArraySeq[String],
    decoders: ArraySeq[CsvDecoder[_]]
  )(construct: scala.Function16[A0, A1, A2, A3, A4, A5, A6, A7, A8, A9, A10, A11, A12, A13, A14, A15, Target]): CsvDecoder[Target] = {
    new CsvDecoder[Target] {
      override val isOptional = false
      override val isPrimitive = false
      override def parseAsOption(row: Map[String, String], columnName: Option[String]): Either[List[CsvDecoderError], Option[Target]] = {
        parseProductAsOption(names, decoders, row, columnName, options) match {
          case Left(errors) => Left(errors)
          case Right(None) => Right(None)
          case Right(Some(results)) =>
            Right(
              Some(
                construct(
                  results(0).asInstanceOf[A0],
                  results(1).asInstanceOf[A1],
                  results(2).asInstanceOf[A2],
                  results(3).asInstanceOf[A3],
                  results(4).asInstanceOf[A4],
                  results(5).asInstanceOf[A5],
                  results(6).asInstanceOf[A6],
                  results(7).asInstanceOf[A7],
                  results(8).asInstanceOf[A8],
                  results(9).asInstanceOf[A9],
                  results(10).asInstanceOf[A10],
                  results(11).asInstanceOf[A11],
                  results(12).asInstanceOf[A12],
                  results(13).asInstanceOf[A13],
                  results(14).asInstanceOf[A14],
                  results(15).asInstanceOf[A15]
                )
              )
            )
        }
      }

      override def parse(row: Map[String, String], columnName: Option[String]) = {
        val errors = new ListBuffer[CsvDecoderError]
        val results = new Array[Any](names.length)
        var i = 0
        while (i < names.length) {
          val decoder = decoders(i)
          val fieldName = names(i)
          decoder.parse(row, Some(options.nestedFieldLabel(columnName, fieldName))) match {
            case Right(value) => val _ = results(i) = value
            case Left(errs) => val _ = errors.addAll(errs)
          }
          i += 1
        }
        if (errors.nonEmpty) Left(errors.toList)
        else {
          Right(
            construct(
              results(0).asInstanceOf[A0],
              results(1).asInstanceOf[A1],
              results(2).asInstanceOf[A2],
              results(3).asInstanceOf[A3],
              results(4).asInstanceOf[A4],
              results(5).asInstanceOf[A5],
              results(6).asInstanceOf[A6],
              results(7).asInstanceOf[A7],
              results(8).asInstanceOf[A8],
              results(9).asInstanceOf[A9],
              results(10).asInstanceOf[A10],
              results(11).asInstanceOf[A11],
              results(12).asInstanceOf[A12],
              results(13).asInstanceOf[A13],
              results(14).asInstanceOf[A14],
              results(15).asInstanceOf[A15]
            )
          )
        }
      }
    }
  }

  final def forProduct17[Target, A0, A1, A2, A3, A4, A5, A6, A7, A8, A9, A10, A11, A12, A13, A14, A15, A16](
    options: CsvCodecOptions,
    names: ArraySeq[String],
    decoders: ArraySeq[CsvDecoder[_]]
  )(construct: scala.Function17[A0, A1, A2, A3, A4, A5, A6, A7, A8, A9, A10, A11, A12, A13, A14, A15, A16, Target]): CsvDecoder[Target] = {
    new CsvDecoder[Target] {
      override val isOptional = false
      override val isPrimitive = false
      override def parseAsOption(row: Map[String, String], columnName: Option[String]): Either[List[CsvDecoderError], Option[Target]] = {
        parseProductAsOption(names, decoders, row, columnName, options) match {
          case Left(errors) => Left(errors)
          case Right(None) => Right(None)
          case Right(Some(results)) =>
            Right(
              Some(
                construct(
                  results(0).asInstanceOf[A0],
                  results(1).asInstanceOf[A1],
                  results(2).asInstanceOf[A2],
                  results(3).asInstanceOf[A3],
                  results(4).asInstanceOf[A4],
                  results(5).asInstanceOf[A5],
                  results(6).asInstanceOf[A6],
                  results(7).asInstanceOf[A7],
                  results(8).asInstanceOf[A8],
                  results(9).asInstanceOf[A9],
                  results(10).asInstanceOf[A10],
                  results(11).asInstanceOf[A11],
                  results(12).asInstanceOf[A12],
                  results(13).asInstanceOf[A13],
                  results(14).asInstanceOf[A14],
                  results(15).asInstanceOf[A15],
                  results(16).asInstanceOf[A16]
                )
              )
            )
        }
      }

      override def parse(row: Map[String, String], columnName: Option[String]) = {
        val errors = new ListBuffer[CsvDecoderError]
        val results = new Array[Any](names.length)
        var i = 0
        while (i < names.length) {
          val decoder = decoders(i)
          val fieldName = names(i)
          decoder.parse(row, Some(options.nestedFieldLabel(columnName, fieldName))) match {
            case Right(value) => val _ = results(i) = value
            case Left(errs) => val _ = errors.addAll(errs)
          }
          i += 1
        }
        if (errors.nonEmpty) Left(errors.toList)
        else {
          Right(
            construct(
              results(0).asInstanceOf[A0],
              results(1).asInstanceOf[A1],
              results(2).asInstanceOf[A2],
              results(3).asInstanceOf[A3],
              results(4).asInstanceOf[A4],
              results(5).asInstanceOf[A5],
              results(6).asInstanceOf[A6],
              results(7).asInstanceOf[A7],
              results(8).asInstanceOf[A8],
              results(9).asInstanceOf[A9],
              results(10).asInstanceOf[A10],
              results(11).asInstanceOf[A11],
              results(12).asInstanceOf[A12],
              results(13).asInstanceOf[A13],
              results(14).asInstanceOf[A14],
              results(15).asInstanceOf[A15],
              results(16).asInstanceOf[A16]
            )
          )
        }
      }
    }
  }

  final def forProduct18[Target, A0, A1, A2, A3, A4, A5, A6, A7, A8, A9, A10, A11, A12, A13, A14, A15, A16, A17](
    options: CsvCodecOptions,
    names: ArraySeq[String],
    decoders: ArraySeq[CsvDecoder[_]]
  )(construct: scala.Function18[A0, A1, A2, A3, A4, A5, A6, A7, A8, A9, A10, A11, A12, A13, A14, A15, A16, A17, Target]): CsvDecoder[Target] = {
    new CsvDecoder[Target] {
      override val isOptional = false
      override val isPrimitive = false
      override def parseAsOption(row: Map[String, String], columnName: Option[String]): Either[List[CsvDecoderError], Option[Target]] = {
        parseProductAsOption(names, decoders, row, columnName, options) match {
          case Left(errors) => Left(errors)
          case Right(None) => Right(None)
          case Right(Some(results)) =>
            Right(
              Some(
                construct(
                  results(0).asInstanceOf[A0],
                  results(1).asInstanceOf[A1],
                  results(2).asInstanceOf[A2],
                  results(3).asInstanceOf[A3],
                  results(4).asInstanceOf[A4],
                  results(5).asInstanceOf[A5],
                  results(6).asInstanceOf[A6],
                  results(7).asInstanceOf[A7],
                  results(8).asInstanceOf[A8],
                  results(9).asInstanceOf[A9],
                  results(10).asInstanceOf[A10],
                  results(11).asInstanceOf[A11],
                  results(12).asInstanceOf[A12],
                  results(13).asInstanceOf[A13],
                  results(14).asInstanceOf[A14],
                  results(15).asInstanceOf[A15],
                  results(16).asInstanceOf[A16],
                  results(17).asInstanceOf[A17]
                )
              )
            )
        }
      }

      override def parse(row: Map[String, String], columnName: Option[String]) = {
        val errors = new ListBuffer[CsvDecoderError]
        val results = new Array[Any](names.length)
        var i = 0
        while (i < names.length) {
          val decoder = decoders(i)
          val fieldName = names(i)
          decoder.parse(row, Some(options.nestedFieldLabel(columnName, fieldName))) match {
            case Right(value) => val _ = results(i) = value
            case Left(errs) => val _ = errors.addAll(errs)
          }
          i += 1
        }
        if (errors.nonEmpty) Left(errors.toList)
        else {
          Right(
            construct(
              results(0).asInstanceOf[A0],
              results(1).asInstanceOf[A1],
              results(2).asInstanceOf[A2],
              results(3).asInstanceOf[A3],
              results(4).asInstanceOf[A4],
              results(5).asInstanceOf[A5],
              results(6).asInstanceOf[A6],
              results(7).asInstanceOf[A7],
              results(8).asInstanceOf[A8],
              results(9).asInstanceOf[A9],
              results(10).asInstanceOf[A10],
              results(11).asInstanceOf[A11],
              results(12).asInstanceOf[A12],
              results(13).asInstanceOf[A13],
              results(14).asInstanceOf[A14],
              results(15).asInstanceOf[A15],
              results(16).asInstanceOf[A16],
              results(17).asInstanceOf[A17]
            )
          )
        }
      }
    }
  }

  final def forProduct19[Target, A0, A1, A2, A3, A4, A5, A6, A7, A8, A9, A10, A11, A12, A13, A14, A15, A16, A17, A18](
    options: CsvCodecOptions,
    names: ArraySeq[String],
    decoders: ArraySeq[CsvDecoder[_]]
  )(construct: scala.Function19[A0, A1, A2, A3, A4, A5, A6, A7, A8, A9, A10, A11, A12, A13, A14, A15, A16, A17, A18, Target]): CsvDecoder[Target] = {
    new CsvDecoder[Target] {
      override val isOptional = false
      override val isPrimitive = false
      override def parseAsOption(row: Map[String, String], columnName: Option[String]): Either[List[CsvDecoderError], Option[Target]] = {
        parseProductAsOption(names, decoders, row, columnName, options) match {
          case Left(errors) => Left(errors)
          case Right(None) => Right(None)
          case Right(Some(results)) =>
            Right(
              Some(
                construct(
                  results(0).asInstanceOf[A0],
                  results(1).asInstanceOf[A1],
                  results(2).asInstanceOf[A2],
                  results(3).asInstanceOf[A3],
                  results(4).asInstanceOf[A4],
                  results(5).asInstanceOf[A5],
                  results(6).asInstanceOf[A6],
                  results(7).asInstanceOf[A7],
                  results(8).asInstanceOf[A8],
                  results(9).asInstanceOf[A9],
                  results(10).asInstanceOf[A10],
                  results(11).asInstanceOf[A11],
                  results(12).asInstanceOf[A12],
                  results(13).asInstanceOf[A13],
                  results(14).asInstanceOf[A14],
                  results(15).asInstanceOf[A15],
                  results(16).asInstanceOf[A16],
                  results(17).asInstanceOf[A17],
                  results(18).asInstanceOf[A18]
                )
              )
            )
        }
      }

      override def parse(row: Map[String, String], columnName: Option[String]) = {
        val errors = new ListBuffer[CsvDecoderError]
        val results = new Array[Any](names.length)
        var i = 0
        while (i < names.length) {
          val decoder = decoders(i)
          val fieldName = names(i)
          decoder.parse(row, Some(options.nestedFieldLabel(columnName, fieldName))) match {
            case Right(value) => val _ = results(i) = value
            case Left(errs) => val _ = errors.addAll(errs)
          }
          i += 1
        }
        if (errors.nonEmpty) Left(errors.toList)
        else {
          Right(
            construct(
              results(0).asInstanceOf[A0],
              results(1).asInstanceOf[A1],
              results(2).asInstanceOf[A2],
              results(3).asInstanceOf[A3],
              results(4).asInstanceOf[A4],
              results(5).asInstanceOf[A5],
              results(6).asInstanceOf[A6],
              results(7).asInstanceOf[A7],
              results(8).asInstanceOf[A8],
              results(9).asInstanceOf[A9],
              results(10).asInstanceOf[A10],
              results(11).asInstanceOf[A11],
              results(12).asInstanceOf[A12],
              results(13).asInstanceOf[A13],
              results(14).asInstanceOf[A14],
              results(15).asInstanceOf[A15],
              results(16).asInstanceOf[A16],
              results(17).asInstanceOf[A17],
              results(18).asInstanceOf[A18]
            )
          )
        }
      }
    }
  }

  final def forProduct20[Target, A0, A1, A2, A3, A4, A5, A6, A7, A8, A9, A10, A11, A12, A13, A14, A15, A16, A17, A18, A19](
    options: CsvCodecOptions,
    names: ArraySeq[String],
    decoders: ArraySeq[CsvDecoder[_]]
  )(construct: scala.Function20[A0, A1, A2, A3, A4, A5, A6, A7, A8, A9, A10, A11, A12, A13, A14, A15, A16, A17, A18, A19, Target]): CsvDecoder[Target] = {
    new CsvDecoder[Target] {
      override val isOptional = false
      override val isPrimitive = false
      override def parseAsOption(row: Map[String, String], columnName: Option[String]): Either[List[CsvDecoderError], Option[Target]] = {
        parseProductAsOption(names, decoders, row, columnName, options) match {
          case Left(errors) => Left(errors)
          case Right(None) => Right(None)
          case Right(Some(results)) =>
            Right(
              Some(
                construct(
                  results(0).asInstanceOf[A0],
                  results(1).asInstanceOf[A1],
                  results(2).asInstanceOf[A2],
                  results(3).asInstanceOf[A3],
                  results(4).asInstanceOf[A4],
                  results(5).asInstanceOf[A5],
                  results(6).asInstanceOf[A6],
                  results(7).asInstanceOf[A7],
                  results(8).asInstanceOf[A8],
                  results(9).asInstanceOf[A9],
                  results(10).asInstanceOf[A10],
                  results(11).asInstanceOf[A11],
                  results(12).asInstanceOf[A12],
                  results(13).asInstanceOf[A13],
                  results(14).asInstanceOf[A14],
                  results(15).asInstanceOf[A15],
                  results(16).asInstanceOf[A16],
                  results(17).asInstanceOf[A17],
                  results(18).asInstanceOf[A18],
                  results(19).asInstanceOf[A19]
                )
              )
            )
        }
      }

      override def parse(row: Map[String, String], columnName: Option[String]) = {
        val errors = new ListBuffer[CsvDecoderError]
        val results = new Array[Any](names.length)
        var i = 0
        while (i < names.length) {
          val decoder = decoders(i)
          val fieldName = names(i)
          decoder.parse(row, Some(options.nestedFieldLabel(columnName, fieldName))) match {
            case Right(value) => val _ = results(i) = value
            case Left(errs) => val _ = errors.addAll(errs)
          }
          i += 1
        }
        if (errors.nonEmpty) Left(errors.toList)
        else {
          Right(
            construct(
              results(0).asInstanceOf[A0],
              results(1).asInstanceOf[A1],
              results(2).asInstanceOf[A2],
              results(3).asInstanceOf[A3],
              results(4).asInstanceOf[A4],
              results(5).asInstanceOf[A5],
              results(6).asInstanceOf[A6],
              results(7).asInstanceOf[A7],
              results(8).asInstanceOf[A8],
              results(9).asInstanceOf[A9],
              results(10).asInstanceOf[A10],
              results(11).asInstanceOf[A11],
              results(12).asInstanceOf[A12],
              results(13).asInstanceOf[A13],
              results(14).asInstanceOf[A14],
              results(15).asInstanceOf[A15],
              results(16).asInstanceOf[A16],
              results(17).asInstanceOf[A17],
              results(18).asInstanceOf[A18],
              results(19).asInstanceOf[A19]
            )
          )
        }
      }
    }
  }

  final def forProduct21[Target, A0, A1, A2, A3, A4, A5, A6, A7, A8, A9, A10, A11, A12, A13, A14, A15, A16, A17, A18, A19, A20](
    options: CsvCodecOptions,
    names: ArraySeq[String],
    decoders: ArraySeq[CsvDecoder[_]]
  )(construct: scala.Function21[A0, A1, A2, A3, A4, A5, A6, A7, A8, A9, A10, A11, A12, A13, A14, A15, A16, A17, A18, A19, A20, Target]): CsvDecoder[Target] = {
    new CsvDecoder[Target] {
      override val isOptional = false
      override val isPrimitive = false
      override def parseAsOption(row: Map[String, String], columnName: Option[String]): Either[List[CsvDecoderError], Option[Target]] = {
        parseProductAsOption(names, decoders, row, columnName, options) match {
          case Left(errors) => Left(errors)
          case Right(None) => Right(None)
          case Right(Some(results)) =>
            Right(
              Some(
                construct(
                  results(0).asInstanceOf[A0],
                  results(1).asInstanceOf[A1],
                  results(2).asInstanceOf[A2],
                  results(3).asInstanceOf[A3],
                  results(4).asInstanceOf[A4],
                  results(5).asInstanceOf[A5],
                  results(6).asInstanceOf[A6],
                  results(7).asInstanceOf[A7],
                  results(8).asInstanceOf[A8],
                  results(9).asInstanceOf[A9],
                  results(10).asInstanceOf[A10],
                  results(11).asInstanceOf[A11],
                  results(12).asInstanceOf[A12],
                  results(13).asInstanceOf[A13],
                  results(14).asInstanceOf[A14],
                  results(15).asInstanceOf[A15],
                  results(16).asInstanceOf[A16],
                  results(17).asInstanceOf[A17],
                  results(18).asInstanceOf[A18],
                  results(19).asInstanceOf[A19],
                  results(20).asInstanceOf[A20]
                )
              )
            )
        }
      }

      override def parse(row: Map[String, String], columnName: Option[String]) = {
        val errors = new ListBuffer[CsvDecoderError]
        val results = new Array[Any](names.length)
        var i = 0
        while (i < names.length) {
          val decoder = decoders(i)
          val fieldName = names(i)
          decoder.parse(row, Some(options.nestedFieldLabel(columnName, fieldName))) match {
            case Right(value) => val _ = results(i) = value
            case Left(errs) => val _ = errors.addAll(errs)
          }
          i += 1
        }
        if (errors.nonEmpty) Left(errors.toList)
        else {
          Right(
            construct(
              results(0).asInstanceOf[A0],
              results(1).asInstanceOf[A1],
              results(2).asInstanceOf[A2],
              results(3).asInstanceOf[A3],
              results(4).asInstanceOf[A4],
              results(5).asInstanceOf[A5],
              results(6).asInstanceOf[A6],
              results(7).asInstanceOf[A7],
              results(8).asInstanceOf[A8],
              results(9).asInstanceOf[A9],
              results(10).asInstanceOf[A10],
              results(11).asInstanceOf[A11],
              results(12).asInstanceOf[A12],
              results(13).asInstanceOf[A13],
              results(14).asInstanceOf[A14],
              results(15).asInstanceOf[A15],
              results(16).asInstanceOf[A16],
              results(17).asInstanceOf[A17],
              results(18).asInstanceOf[A18],
              results(19).asInstanceOf[A19],
              results(20).asInstanceOf[A20]
            )
          )
        }
      }
    }
  }

  final def forProduct22[Target, A0, A1, A2, A3, A4, A5, A6, A7, A8, A9, A10, A11, A12, A13, A14, A15, A16, A17, A18, A19, A20, A21](
    options: CsvCodecOptions,
    names: ArraySeq[String],
    decoders: ArraySeq[CsvDecoder[_]]
  )(construct: scala.Function22[A0, A1, A2, A3, A4, A5, A6, A7, A8, A9, A10, A11, A12, A13, A14, A15, A16, A17, A18, A19, A20, A21, Target]): CsvDecoder[Target] = {
    new CsvDecoder[Target] {
      override val isOptional = false
      override val isPrimitive = false
      override def parseAsOption(row: Map[String, String], columnName: Option[String]): Either[List[CsvDecoderError], Option[Target]] = {
        parseProductAsOption(names, decoders, row, columnName, options) match {
          case Left(errors) => Left(errors)
          case Right(None) => Right(None)
          case Right(Some(results)) =>
            Right(
              Some(
                construct(
                  results(0).asInstanceOf[A0],
                  results(1).asInstanceOf[A1],
                  results(2).asInstanceOf[A2],
                  results(3).asInstanceOf[A3],
                  results(4).asInstanceOf[A4],
                  results(5).asInstanceOf[A5],
                  results(6).asInstanceOf[A6],
                  results(7).asInstanceOf[A7],
                  results(8).asInstanceOf[A8],
                  results(9).asInstanceOf[A9],
                  results(10).asInstanceOf[A10],
                  results(11).asInstanceOf[A11],
                  results(12).asInstanceOf[A12],
                  results(13).asInstanceOf[A13],
                  results(14).asInstanceOf[A14],
                  results(15).asInstanceOf[A15],
                  results(16).asInstanceOf[A16],
                  results(17).asInstanceOf[A17],
                  results(18).asInstanceOf[A18],
                  results(19).asInstanceOf[A19],
                  results(20).asInstanceOf[A20],
                  results(21).asInstanceOf[A21]
                )
              )
            )
        }
      }

      override def parse(row: Map[String, String], columnName: Option[String]) = {
        val errors = new ListBuffer[CsvDecoderError]
        val results = new Array[Any](names.length)
        var i = 0
        while (i < names.length) {
          val decoder = decoders(i)
          val fieldName = names(i)
          decoder.parse(row, Some(options.nestedFieldLabel(columnName, fieldName))) match {
            case Right(value) => val _ = results(i) = value
            case Left(errs) => val _ = errors.addAll(errs)
          }
          i += 1
        }
        if (errors.nonEmpty) Left(errors.toList)
        else {
          Right(
            construct(
              results(0).asInstanceOf[A0],
              results(1).asInstanceOf[A1],
              results(2).asInstanceOf[A2],
              results(3).asInstanceOf[A3],
              results(4).asInstanceOf[A4],
              results(5).asInstanceOf[A5],
              results(6).asInstanceOf[A6],
              results(7).asInstanceOf[A7],
              results(8).asInstanceOf[A8],
              results(9).asInstanceOf[A9],
              results(10).asInstanceOf[A10],
              results(11).asInstanceOf[A11],
              results(12).asInstanceOf[A12],
              results(13).asInstanceOf[A13],
              results(14).asInstanceOf[A14],
              results(15).asInstanceOf[A15],
              results(16).asInstanceOf[A16],
              results(17).asInstanceOf[A17],
              results(18).asInstanceOf[A18],
              results(19).asInstanceOf[A19],
              results(20).asInstanceOf[A20],
              results(21).asInstanceOf[A21]
            )
          )
        }
      }
    }
  }

  private def parseProductAsOption(
    names: ArraySeq[String],
    decoders: ArraySeq[CsvDecoder[_]],
    row: Map[String, String],
    columnName: Option[String],
    options: CsvCodecOptions
  ): Either[List[CsvDecoderError], Option[Array[Any]]] = {
    val errors = new ListBuffer[CsvDecoderError]
    val results = new Array[Any](names.length)
    var i = 0
    var someCount = 0
    var optionalNoneCount = 0
    while (i < names.length) {
      val decoder = decoders(i)
      val fieldName = names(i)
      decoder.parseAsOption(row, Some(options.nestedFieldLabel(columnName, fieldName))) match {
        case Right(Some(value)) =>
          someCount += 1
          results(i) = value
        case Right(None) =>
          results(i) = None
          if (decoder.isOptional) optionalNoneCount += 1
        case Left(errs) => val _ = errors.addAll(errs)
      }
      i += 1
    }
    if (errors.nonEmpty) Left(errors.toList)
    else if (someCount == 0) Right(None)
    else if (someCount + optionalNoneCount < names.length) {
      val missingColumns = new ListBuffer[String]
      i = 0
      while (i < names.length) {
        val decoder = decoders(i)
        val fieldName = names(i)
        val nestedField = Some(options.nestedFieldLabel(columnName, fieldName))
        decoder.parseAsOption(row, nestedField) match {
          case Right(None) if !decoder.isOptional =>
            val _ = missingColumns.addAll(CsvDecoder.errorColumn(nestedField, decoder.isPrimitive, options))
          case _ => ()
        }
        i += 1
      }
      Left(List(CsvDecoderError.notAllRequiredColumnsExist(missingColumns.result())))
    }
    else {
      Right(Some(results))
    }
  }

  // scalafmt: { maxColumn = 120, optIn.configStyleArguments = true }
}
