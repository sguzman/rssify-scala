package org.github.sguzman.rss.config

import cats.effect.Sync
import cats.syntax.all.*
import org.github.sguzman.rss.model.*
import toml.Toml
import toml.derivation.auto.*

import java.net.URI
import java.nio.file.{Files, Path}
import java.time.Instant

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

final case class RawConfig(
    app: RawApp,
    domains: Option[Map[String, RawDomain]],
    feeds: List[RawFeed]
)

object ConfigLoader:
  def load[F[_]: Sync](path: Path): F[AppConfig] =
    for
      rawContent <- Sync[F].blocking(new String(Files.readAllBytes(path)))
      parsed <- Sync[F]
        .fromEither(Toml.parseAs[RawConfig](rawContent).leftMap(err => new RuntimeException(err.toString)))
        .adaptError { case e => new RuntimeException(s"TOML parse error: ${e.getMessage}", e) }
      cfg <- Sync[F].delay(toAppConfig(parsed, path))
    yield cfg

  private def toAppConfig(raw: RawConfig, path: Path): AppConfig =
    val feeds = raw.feeds.map { f =>
      val uri = URI(f.url)
      val domain = Option(uri.getHost).getOrElse(throw new IllegalArgumentException(s"Feed ${f.id} missing host"))
      FeedConfig(
        id = f.id,
        url = uri,
        domain = domain,
        basePollSeconds = f.base_poll_seconds.getOrElse(raw.app.default_poll_seconds)
      )
    }
    val mode = parseMode(raw.app.mode)
    AppConfig(
      dbPath = path.getParent match
        case null => Path.of(raw.app.db_path)
        case p    => p.resolve(raw.app.db_path),
      defaultPollSeconds = raw.app.default_poll_seconds,
      maxPollSeconds = raw.app.max_poll_seconds,
      errorBackoffBaseSeconds = raw.app.error_backoff_base_seconds,
      maxErrorBackoffSeconds = raw.app.max_error_backoff_seconds,
      jitterFraction = raw.app.jitter_fraction,
      globalMaxConcurrentRequests = raw.app.global_max_concurrent_requests,
      userAgent = raw.app.user_agent,
      mode = mode,
      domains = raw.domains.getOrElse(Map.empty).view.mapValues(d => DomainConfig(d.max_concurrent_requests)).toMap,
      feeds = feeds
    )

  private def parseMode(rawMode: Option[String]): AppMode =
    rawMode.map(_.toLowerCase) match
      case None | Some("prod") => AppMode.Prod
      case Some("dev")         => AppMode.Dev
      case Some(other) =>
        throw IllegalArgumentException(s"Invalid app.mode '$other' in config. Expected 'dev' or 'prod'.")
