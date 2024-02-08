package io.tofhir.common.util

import org.json4s.CustomSerializer
import org.json4s.JsonAST.{JNull, JString}

import java.time.format.DateTimeFormatter
import java.time.{LocalDate, LocalDateTime}

object JavaDateTimeSerializers {

  private val dateTimeFormatter: DateTimeFormatter = DateTimeFormatter.ofPattern("yyyy[-MM[-dd['T'HH[:mm[:ss[.SSS][XXX]]]]]]")

  case object LocalDateTimeSerializer extends CustomSerializer[LocalDateTime](format => ( {
    case JString(s) =>
      val temporalAccessor = dateTimeFormatter.parseBest(s, LocalDateTime.from(_), LocalDate.from(_))
      temporalAccessor match {
        case ldt: LocalDateTime => ldt
        case ld: LocalDate => ld.atStartOfDay()
      }
    case JNull => null
  }, {
    case d: LocalDateTime => JString(d.format(dateTimeFormatter))
  }))

}
