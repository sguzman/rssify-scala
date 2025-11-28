package org.github.sguzman.rss.time

import java.sql.Timestamp
import java.time.format.DateTimeFormatter
import java.time.{Instant, OffsetDateTime, ZoneId}
import java.util.TimeZone
import scala.util.Try

object Time:
  private val formatter =
    DateTimeFormatter.ofPattern(
      "yyyy-MM-dd HH:mm:ss.SSS z"
    )
  private val dbFormatter =
    DateTimeFormatter.ISO_OFFSET_DATE_TIME

  def formatInstant(
      instant: Instant,
      zoneId: ZoneId
  ): String =
    formatter
      .withZone(zoneId)
      .format(instant)

  def instantToDbString(
      instant: Instant,
      zoneId: ZoneId
  ): String =
    instant.atZone(zoneId).format(dbFormatter)

  def parseDbString(
      value: String
  ): Instant =
    val trimmed = value.trim
    Try(
      OffsetDateTime
        .parse(trimmed, dbFormatter)
        .toInstant
    ).orElse(
      Try(Instant.parse(trimmed))
    ).orElse(
      Try(Timestamp.valueOf(trimmed).toInstant)
    ).orElse(
      Try(Instant.ofEpochMilli(trimmed.toLong))
    ).orElse(
      Try(Instant.ofEpochSecond(trimmed.toLong))
    ).getOrElse(
      throw new IllegalArgumentException(
        s"Unparseable timestamp '$value'"
      )
    )

  def setDefaultTimezone(
      zoneId: ZoneId
  ): Unit =
    TimeZone.setDefault(
      TimeZone.getTimeZone(zoneId)
    )
