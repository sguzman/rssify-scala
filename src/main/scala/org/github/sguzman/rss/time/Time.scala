package org.github.sguzman.rss.time

import java.time.format.DateTimeFormatter
import java.time.{Instant, ZoneId}
import java.util.TimeZone

object Time:
  private val formatter =
    DateTimeFormatter.ofPattern(
      "yyyy-MM-dd HH:mm:ss.SSS z"
    )

  def formatInstant(
      instant: Instant,
      zoneId: ZoneId
  ): String =
    formatter
      .withZone(zoneId)
      .format(instant)

  def setDefaultTimezone(
      zoneId: ZoneId
  ): Unit =
    TimeZone.setDefault(
      TimeZone.getTimeZone(zoneId)
    )
