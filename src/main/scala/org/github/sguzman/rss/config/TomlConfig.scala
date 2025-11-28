package org.github.sguzman.rss.config

import cats.effect.Sync
import cats.syntax.all.*
import org.github.sguzman.rss.model.*
import os.*
import toml.{Codec, Toml}
import toml.derivation.auto.*

import java.net.URI
import java.time.ZoneId

final case class RawApp(
    db_path: String,
    default_poll_seconds: Int,
    max_poll_seconds: Int,
    error_backoff_base_seconds: Int,
    max_error_backoff_seconds: Int,
    jitter_fraction: Double,
    global_max_concurrent_requests: Option[Int],
    user_agent: String,
    mode: Option[String] = None,
    timezone: Option[String] = None
)

final case class RawAppFile(app: RawApp)

final case class RawDomain(max_concurrent_requests: Int)

final case class RawFeed(
    id: String,
    url: String,
    base_poll_seconds: Option[Int]
)

final case class RawDomainsFile(domains: List[RawDomainEntry])

final case class RawDomainEntry(name: String, max_concurrent_requests: Int)

final case class RawFeedsFile(feeds: List[RawFeed])

given Codec[RawAppFile] = Codec.derived
given Codec[RawApp] = Codec.derived
given Codec[RawDomain] = Codec.derived
given Codec[RawDomainEntry] = Codec.derived
given Codec[RawDomainsFile] = Codec.derived
given Codec[RawFeed] = Codec.derived
given Codec[RawFeedsFile] = Codec.derived

object ConfigLoader {
  private val defaultTimezone =
    "America/Mexico_City"

  def load[F[_]: Sync](configPath: os.Path): F[AppConfig] = {
    val baseDir = configPath / os.up
    val domainsPath = baseDir / "domains.toml"
    val feedsDir = baseDir / "feeds"
    for {
      appContent <- Sync[F].blocking(os.read(configPath))
      rawApp <- Sync[F]
        .fromEither(
          Toml
            .parseAs[RawAppFile](appContent)
            .leftMap(err => new RuntimeException(err.toString))
        )
        .adaptError { case e =>
          new RuntimeException(
            s"TOML parse error in app file: ${e.getMessage}",
            e
          )
        }
      domainContent <- Sync[F].blocking(os.read(domainsPath))
      rawDomains <- Sync[F]
        .fromEither(
          Toml
            .parseAs[RawDomainsFile](domainContent)
            .leftMap(err => new RuntimeException(err.toString))
        )
        .adaptError { case e =>
          new RuntimeException(
            s"TOML parse error in domains file: ${e.getMessage}",
            e
          )
        }
      rawFeeds <- loadAllFeeds[F](feedsDir)
      cfg <- Sync[F].delay(
        toAppConfig(rawApp.app, rawDomains, rawFeeds, configPath)
      )
    } yield cfg
  }

  private def toAppConfig(
      rawApp: RawApp,
      rawDomains: RawDomainsFile,
      rawFeeds: RawFeedsFile,
      path: os.Path
  ): AppConfig = {
    val feeds = rawFeeds.feeds.map { f =>
      val uri = URI(f.url)
      val domain = Option(uri.getHost).getOrElse(
        throw new IllegalArgumentException(s"Feed ${f.id} missing host")
      )
      FeedConfig(
        id = f.id,
        url = uri,
        domain = domain,
        basePollSeconds =
          f.base_poll_seconds.getOrElse(rawApp.default_poll_seconds)
      )
    }
    val mode = parseMode(rawApp.mode)
    val dbBaseDir =
      try {
        val rel = path.relativeTo(os.pwd)
        if rel.segments.contains("resources") then os.pwd else path / os.up
      } catch {
        case _: IllegalArgumentException => path / os.up
      }
    val dbOsPath = os.Path(rawApp.db_path, base = dbBaseDir)
    val domains = rawDomains.domains
      .map(d => d.name -> DomainConfig(d.max_concurrent_requests))
      .toMap
    val timezone = parseZone(rawApp.timezone)
    AppConfig(
      dbPath = dbOsPath.toNIO,
      defaultPollSeconds = rawApp.default_poll_seconds,
      maxPollSeconds = rawApp.max_poll_seconds,
      errorBackoffBaseSeconds = rawApp.error_backoff_base_seconds,
      maxErrorBackoffSeconds = rawApp.max_error_backoff_seconds,
      jitterFraction = rawApp.jitter_fraction,
      globalMaxConcurrentRequests = rawApp.global_max_concurrent_requests,
      userAgent = rawApp.user_agent,
      mode = mode,
      timezone = timezone,
      domains = domains,
      feeds = feeds
    )
  }

  private def loadAllFeeds[F[_]: Sync](
      feedsDir: os.Path
  ): F[RawFeedsFile] =
    for
      files <- Sync[F].blocking {
        if !os.exists(feedsDir) then
          throw RuntimeException(
            s"Feeds directory not found at $feedsDir"
          )
        os.list(feedsDir)
          .filter(p =>
            os.isFile(p) && p.ext
              .toLowerCase == "toml"
          )
          .sortBy(_.toString)
      }
      _ <- Sync[F].raiseWhen(files.isEmpty)(
        RuntimeException(
          s"No feed files found in $feedsDir"
        )
      )
      parsed <- files.traverse { path =>
        Sync[F]
          .fromEither(
            Toml
              .parseAs[RawFeedsFile](
                os.read(path)
              )
              .leftMap(err =>
                new RuntimeException(
                  err.toString
                )
              )
          )
          .adaptError { case e =>
            new RuntimeException(
              s"TOML parse error in feeds file ${path.last}: ${e.getMessage}",
              e
            )
          }
      }
    yield RawFeedsFile(parsed.flatMap(_.feeds))

  private def parseMode(rawMode: Option[String]): AppMode =
    rawMode.map(_.toLowerCase) match {
      case None | Some("prod") => AppMode.Prod
      case Some("dev")         => AppMode.Dev
      case Some(other)         =>
        throw IllegalArgumentException(
          s"Invalid app.mode '$other' in config. Expected 'dev' or 'prod'."
        )
    }

  private def parseZone(
      rawZone: Option[String]
  ): ZoneId =
    val zoneStr =
      rawZone.filter(_.nonEmpty).getOrElse(defaultTimezone)
    try ZoneId.of(zoneStr)
    catch
      case e: Exception =>
        throw IllegalArgumentException(
          s"Invalid timezone '$zoneStr' in config. See java.time.ZoneId for valid values.",
          e
        )

}
