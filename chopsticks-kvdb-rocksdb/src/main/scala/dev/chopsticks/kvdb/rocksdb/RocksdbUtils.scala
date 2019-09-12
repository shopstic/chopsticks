package dev.chopsticks.kvdb.rocksdb

import java.util.Properties

import com.typesafe.scalalogging.StrictLogging

object RocksdbUtils extends StrictLogging {
  final case class OptionsFileSection(name: String, values: Map[String, String] = Map.empty[String, String])

  private val optionsSectionRegex = """^\[(.+)\]$""".r
  private val optionsValueRegex = """^([^=]+)=(.+)$""".r

  def parseOptions(lines: Traversable[String]): List[OptionsFileSection] = {
    val (all, lastSection) = lines
      .map(_.trim)
      .foldLeft[(List[OptionsFileSection], Option[OptionsFileSection])]((List.empty[OptionsFileSection], None)) {
        case (state @ (list, section), line) =>
          line match {
            case optionsSectionRegex(name, _*) =>
              val s = Some(OptionsFileSection(name))

              if (section.nonEmpty) (list :+ section.get, s)
              else (list, s)

            case optionsValueRegex(key, value, _*) =>
              (list, section.map { s =>
                s.copy(values = s.values.updated(key, value))
              })
            case _ => state
          }
      }

    lastSection.map(s => all :+ s).getOrElse(all)
  }

  def propertiesFromMap(map: Map[String, String]): Properties = {
    map.foldLeft(new Properties) {
      case (a, (k, v)) =>
        val _ = a.put(k, v)
        a
    }
  }
}
