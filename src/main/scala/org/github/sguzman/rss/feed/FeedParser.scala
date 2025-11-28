package org.github.sguzman.rss.feed

import java.nio.charset.StandardCharsets
import java.time.format.DateTimeFormatter
import java.time.{Instant, OffsetDateTime, ZonedDateTime}
import scala.util.Try
import scala.xml.{Elem, NodeSeq, XML}

object FeedParser:
  // Relax XML entity size limits so large feeds can be parsed.
  private val configureXmlLimits: Unit =
    List(
      "jdk.xml.maxGeneralEntitySizeLimit",
      "jdk.xml.totalEntitySizeLimit"
    ).foreach { key =>
      if System.getProperty(key) == null then
        System.setProperty(key, "0")
    }

  final case class FeedMetadata(
      title: Option[String],
      link: Option[String],
      description: Option[String],
      language: Option[String],
      updatedAt: Option[Instant]
  )

  final case class FeedItem(
      title: Option[String],
      link: Option[String],
      guid: Option[String],
      author: Option[String],
      publishedAt: Option[Instant],
      updatedAt: Option[Instant],
      summary: Option[String],
      content: Option[String]
  )

  final case class ParsedFeed(
      metadata: FeedMetadata,
      items: List[FeedItem]
  )

  def parse(
      bytes: Array[Byte]
  ): Either[Throwable, ParsedFeed] =
    configureXmlLimits
    Try {
      val xmlString =
        String(bytes, StandardCharsets.UTF_8)
      XML.loadString(xmlString)
    }.toEither
      .flatMap(parseXml)

  private def parseXml(
      xml: Elem
  ): Either[Throwable, ParsedFeed] =
    Try {
      val isAtom =
        xml.label.equalsIgnoreCase("feed") || xml.label
          .toLowerCase
          .contains("feed")
      if isAtom then parseAtom(xml)
      else parseRss(xml)
    }.toEither

  private def parseRss(xml: Elem): ParsedFeed =
    val channel =
      (xml \\ "channel").headOption.getOrElse(xml)

    def channelText(name: String) =
      text(channel \ name)

    val meta = FeedMetadata(
      title = channelText("title"),
      link = channelText("link"),
      description = channelText("description"),
      language = channelText("language"),
      updatedAt = channelText("lastBuildDate")
        .orElse(channelText("pubDate"))
        .flatMap(parseDate)
    )

    val items = (channel \ "item").toList.map { item =>
      def itemText(name: String) =
        text(item \ name)
      FeedItem(
        title = itemText("title"),
        link = itemText("link"),
        guid = itemText("guid").orElse(
          itemText("id")
        ),
        author = itemText("author"),
        publishedAt = itemText("pubDate")
          .flatMap(parseDate),
        updatedAt = itemText("updated")
          .flatMap(parseDate),
        summary = itemText("description"),
        content = text(item \\ "encoded")
          .orElse(itemText("content"))
      )
    }
    ParsedFeed(meta, items)

  private def parseAtom(xml: Elem): ParsedFeed =
    val meta = FeedMetadata(
      title = text(xml \ "title"),
      link = extractAtomLink(xml),
      description = text(xml \ "subtitle"),
      language = text(xml \ "language"),
      updatedAt = text(xml \ "updated")
        .flatMap(parseDate)
    )
    val items =
      (xml \ "entry").toList.map { entry =>
        FeedItem(
          title = text(entry \ "title"),
          link = extractAtomLink(entry),
          guid = text(entry \ "id"),
          author = text(entry \ "author" \ "name"),
          publishedAt = text(entry \ "published")
            .flatMap(parseDate),
          updatedAt = text(entry \ "updated")
            .flatMap(parseDate),
          summary = text(entry \ "summary"),
          content = text(entry \ "content")
        )
      }
    ParsedFeed(meta, items)

  private def text(ns: NodeSeq): Option[String] =
    ns.headOption
      .map(_.text.trim)
      .filter(_.nonEmpty)

  private def extractAtomLink(
      ns: NodeSeq
  ): Option[String] =
    val relAlt = (ns \ "link")
      .find(n =>
        n.attribute("rel")
          .exists(_.text == "alternate")
      )
      .flatMap(n => n.attribute("href"))
      .map(_.text.trim)
    relAlt.orElse(
      (ns \ "link").headOption
        .flatMap(n =>
          n.attribute("href")
            .map(_.text.trim)
            .filter(_.nonEmpty)
        )
    )

  private def parseDate(
      value: String
  ): Option[Instant] =
    val trimmed = value.trim
    parseWith(
      () => OffsetDateTime
        .parse(
          trimmed,
          DateTimeFormatter.ISO_OFFSET_DATE_TIME
        )
        .toInstant
    ).orElse(
      parseWith(() =>
        ZonedDateTime
          .parse(
            trimmed,
            DateTimeFormatter.RFC_1123_DATE_TIME
          )
          .toInstant
      )
    ).orElse(
      parseWith(() => Instant.parse(trimmed))
    )

  private def parseWith(
      thunk: () => Instant
  ): Option[Instant] =
    Try(thunk()).toOption
