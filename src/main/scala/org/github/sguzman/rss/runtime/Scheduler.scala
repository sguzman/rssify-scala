package org.github.sguzman.rss.runtime

import cats.effect.kernel.{Async, Clock}
import cats.effect.std.{Random, Semaphore}
import cats.syntax.all.*
import fs2.Stream
import org.github.sguzman.rss.Logging
import org.github.sguzman.rss.db.Database
import org.github.sguzman.rss.http.HttpClient
import org.github.sguzman.rss.model.*
import org.http4s.Status
import org.http4s.Uri
import org.http4s.client.Client
import org.http4s.syntax.all.*
import org.typelevel.log4cats.Logger

import java.net.URI
import java.time.Instant
import scala.concurrent.duration.*

object Scheduler:
  private val tickInterval = 5.seconds

  def makeSemaphores[F[_]: Async](cfg: AppConfig): F[(Option[Semaphore[F]], Map[String, Semaphore[F]])] =
    val global = cfg.globalMaxConcurrentRequests.traverse(Semaphore[F](_))
    val perDomain = cfg.domains.toList.traverse { case (domain, dcfg) =>
      Semaphore[F](dcfg.maxConcurrentRequests).map(domain -> _)
    }
    (global, perDomain.map(_.toMap)).tupled

  def loop[F[_]: Async: Logger](
      cfg: AppConfig,
      client: Client[F],
      xa: doobie.Transactor[F]
  ): Stream[F, Unit] =
    Stream.eval(makeSemaphores(cfg)).flatMap { case (globalSem, domainSems) =>
      Stream.awakeEvery[F](tickInterval).evalMap { _ =>
        tick(cfg, client, xa, globalSem, domainSems)
      }
    }

  private def tick[F[_]: Async: Logger](
      cfg: AppConfig,
      client: Client[F],
      xa: doobie.Transactor[F],
      globalSem: Option[Semaphore[F]],
      domainSems: Map[String, Semaphore[F]]
  ): F[Unit] =
    for
      now <- Clock[F].realTimeInstant
      due <- Database.dueFeeds(now, cfg.feeds, xa)
      _ <- Logger[F].info(s"Scheduler tick at $now: ${due.size} feeds due")
      _ <- due.parTraverse_(feed => processFeed(cfg, client, xa, feed, now, globalSem, domainSems))
    yield ()

  private def processFeed[F[_]: Async: Logger](
      cfg: AppConfig,
      client: Client[F],
      xa: doobie.Transactor[F],
      feed: FeedConfig,
      now: Instant,
      globalSem: Option[Semaphore[F]],
      domainSems: Map[String, Semaphore[F]]
  ): F[Unit] =
    for
      rng <- Random.scalaUtilRandom[F]
      rand <- rng.nextDouble
      stored <- Database.latestState(feed.id, xa)
      state = stored.flatMap(toLinkState(_, cfg)).getOrElse(LinkState.initial(feed, cfg, now))
      action = LinkState.decideNextAction(state, now)
      _ <- Logger[F].debug(s"Feed ${feed.id} action: $action at $now")
      _ <- action match
        case NextAction.SleepUntil(_) => Async[F].unit
        case NextAction.DoHead(_)     => doHead(cfg, client, xa, feed, state, now, rand, globalSem, domainSems)
        case NextAction.DoGet(_)      => doGet(cfg, client, xa, feed, state, now, rand, globalSem, domainSems)
    yield ()

  private def doHead[F[_]: Async: Logger](
      cfg: AppConfig,
      client: Client[F],
      xa: doobie.Transactor[F],
      feed: FeedConfig,
      state: LinkState,
      now: Instant,
      rand: Double,
      globalSem: Option[Semaphore[F]],
      domainSems: Map[String, Semaphore[F]]
  ): F[Unit] =
    getDomainSem(feed.domain, domainSems).flatMap { domainSem =>
      val acquire = combinedPermit(globalSem, domainSem)
      acquire.use { _ =>
        val uri = Uri.unsafeFromString(feed.url.toString)
        for
          res <- HttpClient.doHead(client, uri, cfg.userAgent)
          updated = LinkState.applyHeadResult(state.copy(phase = LinkPhase.NeedsHead), res, now, rand)
          _ <- Database.insertEvent(
            feedId = feed.id,
            method = "HEAD",
            status = res.status.map(_.code),
            errorKind = res.error,
            latency = Some(res.latency.toMillis),
            backoffIndex = updated.backoffIndex,
            scheduled = updated.nextActionAt,
            debug = updated.note,
            xa = xa
          )
          _ <- Database.insertState(updated, now, xa)
        yield ()
      }
    }

  private def doGet[F[_]: Async: Logger](
      cfg: AppConfig,
      client: Client[F],
      xa: doobie.Transactor[F],
      feed: FeedConfig,
      state: LinkState,
      now: Instant,
      rand: Double,
      globalSem: Option[Semaphore[F]],
      domainSems: Map[String, Semaphore[F]]
  ): F[Unit] =
    getDomainSem(feed.domain, domainSems).flatMap { domainSem =>
      val acquire = combinedPermit(globalSem, domainSem)
      acquire.use { _ =>
        val uri = Uri.unsafeFromString(feed.url.toString)
        for
          res <- HttpClient.doGet(client, uri, cfg.userAgent)
          bodyChanged = res.body.exists(_.nonEmpty) // heuristic
          updated = LinkState.applyGetResult(state.copy(phase = LinkPhase.NeedsGet), res, now, bodyChanged, rand)
        _ <- Database.insertEvent(
          feedId = feed.id,
          method = "GET",
          status = res.status.map(_.code),
          errorKind = res.error,
          latency = Some(res.latency.toMillis),
          backoffIndex = updated.backoffIndex,
          scheduled = updated.nextActionAt,
          debug = updated.note,
          xa = xa
        )
        _ <- res.body.traverse_ { b =>
          val hash = Hashing.sha256(b)
          Database.insertBody(feed.id, now, res.etag, res.lastModified, Some(hash), b, xa)
        }
          _ <- Database.insertState(updated, now, xa)
        yield ()
      }
    }

  private def toLinkState(row: Database.StateRow, cfg: AppConfig): Option[LinkState] =
    for
      phase <- parsePhase(row.phase)
    yield LinkState(
      feedId = row.feedId,
      phase = phase,
      lastHeadAt = row.lastHeadAt,
      lastHeadStatus = row.lastHeadStatus.flatMap(Status.fromInt(_).toOption),
      lastHeadError = row.lastHeadError.flatMap(parseError),
      lastGetAt = row.lastGetAt,
      lastGetStatus = row.lastGetStatus.flatMap(Status.fromInt(_).toOption),
      lastGetError = row.lastGetError.flatMap(parseError),
      etag = row.etag,
      lastModified = row.lastModified,
      backoffIndex = row.backoffIndex,
      basePollSeconds = row.basePollSeconds,
      maxPollSeconds = cfg.maxPollSeconds,
      jitterFraction = cfg.jitterFraction,
      nextActionAt = row.nextActionAt,
      jitterSeconds = row.jitterSeconds,
      note = row.note
    )

  private def parseError(s: String): Option[ErrorKind] =
    s match
      case "Timeout"           => Some(ErrorKind.Timeout)
      case "DnsFailure"        => Some(ErrorKind.DnsFailure)
      case "ConnectionFailure" => Some(ErrorKind.ConnectionFailure)
      case "Http4xx"           => Some(ErrorKind.Http4xx)
      case "Http5xx"           => Some(ErrorKind.Http5xx)
      case "ParseError"        => Some(ErrorKind.ParseError)
      case "Unexpected"        => Some(ErrorKind.Unexpected)
      case _                   => None

  private def parsePhase(s: String): Option[LinkPhase] =
    s match
      case "NeedsInitialGet" => Some(LinkPhase.NeedsInitialGet)
      case "NeedsHead"       => Some(LinkPhase.NeedsHead)
      case "NeedsGet"        => Some(LinkPhase.NeedsGet)
      case "Sleeping"        => Some(LinkPhase.Sleeping)
      case "ErrorBackoff"    => Some(LinkPhase.ErrorBackoff)
      case _                 => None

  private def getDomainSem[F[_]: Async](domain: String, existing: Map[String, Semaphore[F]]): F[Semaphore[F]] =
    existing.get(domain).fold(Semaphore[F](1))(Async[F].pure)

  private def combinedPermit[F[_]: Async](global: Option[Semaphore[F]], domain: Semaphore[F]): cats.effect.Resource[F, Unit] =
    global match
      case Some(g) => for _ <- g.permit; _ <- domain.permit yield ()
      case None    => domain.permit
