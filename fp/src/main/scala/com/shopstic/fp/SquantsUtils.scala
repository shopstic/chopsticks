package com.shopstic.fp
import squants.Quantity
import squants.experimental.formatter.DefaultFormatter
import squants.experimental.unitgroups.information.IECInformation
import squants.experimental.unitgroups.si.expanded.time.ExpandedSiTimes
import squants.information.Information
import squants.time.Time

object SquantsUtils {
  object Implicits {
    implicit val timeFormatter: DefaultFormatter[Time] = new DefaultFormatter(ExpandedSiTimes)
    implicit val informationFormatter: DefaultFormatter[Information] = new DefaultFormatter(IECInformation)
    implicit class SquantsInBestUnit[A <: Quantity[A]](q: Quantity[A]) {
      def inBestUnit(implicit formatter: DefaultFormatter[A]): A = {
        formatter.inBestUnit(q)
      }
    }
  }
}
