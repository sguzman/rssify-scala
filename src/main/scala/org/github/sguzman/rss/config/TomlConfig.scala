package org.github.sguzman.rss.config

import cats.effect.Sync
import cats.syntax.all.*
import org.github.sguzman.rss.model.*
import os.*
import toml.{Codec, Toml}
import toml.derivation.auto.*

import java.net.URI

final case class RawApp(
    db_path: String,
    default_poll_seconds: Int,
    max_poll_seconds: Int,
    error_backoff_base_seconds: Int,
    max_error_backoff_seconds: Int,
    jitter_fraction: Double,
    global_max_concurrent_requests: Option[Int],
    user_agent: String,
    mode: Option[String] = None
)

final case class RawDomain(max_concurrent_requests: Int)

final case class RawFeed(id: String, url: String, base_poll_seconds: Option[Int])

final case class RawDomainsFile(domains: List[RawDomainEntry])

final case class RawDomainEntry(name: String, max_concurrent_requests: Int)

final case class RawFeedsFile(feeds: List[RawFeed])

given Codec[RawApp] = Codec.derived
given Codec[RawDomain] = Codec.derived
given Codec[RawDomainEntry] = Codec.derived
given Codec[RawDomainsFile] = Codec.derived
given Codec[RawFeed] = Codec.derived
given Codec[RawFeedsFile] = Codec.derived

object ConfigLoader {
  def load[F[_]: Sync](configPath: os.Path): F[AppConfig] = {
    val baseDir = configPath / os.up
    val domainsPath = baseDir / "domains.toml"
    val feedsPath = baseDir / "feeds.toml"
    for {
      appContent <- Sync[F].blocking(os.read(configPath))
      rawApp <- Sync[F]
        .fromEither(Toml.parseAs[RawApp](appContent).leftMap(err => new RuntimeException(err.toString)))
        .adaptError { case e => new RuntimeException(s"TOML parse error in app file: ${e.getMessage}", e) }
      domainContent <- Sync[F].blocking(os.read(domainsPath))
      rawDomains <- Sync[F]
        .fromEither(Toml.parseAs[RawDomainsFile](domainContent).leftMap(err => new RuntimeException(err.toString)))
        .adaptError { case e => new RuntimeException(s"TOML parse error in domains file: ${e.getMessage}", e) }
      feedContent <- Sync[F].blocking(os.read(feedsPath))
      rawFeeds <- Sync[F]
        .fromEither(Toml.parseAs[RawFeedsFile](feedContent).leftMap(err => new RuntimeException(err.toString)))
        .adaptError { case e => new RuntimeException(s"TOML parse error in feeds file: ${e.getMessage}", e) }
      cfg <- Sync[F].delay(toAppConfig(rawApp, rawDomains, rawFeeds, configPath))
    } yield cfg
  }

  private def toAppConfig(rawApp: RawApp, rawDomains: RawDomainsFile, rawFeeds: RawFeedsFile, path: os.Path): AppConfig = {
    val feeds = rawFeeds.feeds.map { f =>
      val uri = URI(f.url)
      val domain = Option(uri.getHost).getOrElse(throw new IllegalArgumentException(s"Feed ${f.id} missing host"))
      FeedConfig(
        id = f.id,
        url = uri,
        domain = domain,
        basePollSeconds = f.base_poll_seconds.getOrElse(rawApp.default_poll_seconds)
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
    val domains = rawDomains.domains.map(d => d.name -> DomainConfig(d.max_concurrent_requests)).toMap
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
      domains = domains,
      feeds = feeds
    )
  }

  private def parseMode(rawMode: Option[String]): AppMode =
    rawMode.map(_.toLowerCase) match {
      case None | Some("prod") => AppMode.Prod
      case Some("dev")         => AppMode.Dev
      case Some(other) =>
        throw IllegalArgumentException(s"Invalid app.mode '$other' in config. Expected 'dev' or 'prod'.")
    }

}
