package org.github.sguzman.rss.model

import cats.implicits.*
import org.http4s.Status

import java.net.URI
import java.nio.file.Path
import java.security.MessageDigest
import java.time.{Instant, ZoneId}
import scala.concurrent.duration.*

// ----- Configuration -----

final case class DomainConfig(maxConcurrentRequests: Int)

final case class FeedConfig(
    id: String,
    url: URI,
    domain: String,
    basePollSeconds: Int
)

final case class AppConfig(
    dbPath: Path,
    defaultPollSeconds: Int,
    maxPollSeconds: Int,
    errorBackoffBaseSeconds: Int,
    maxErrorBackoffSeconds: Int,
    jitterFraction: Double,
    globalMaxConcurrentRequests: Option[Int],
    userAgent: String,
    domains: Map[String, DomainConfig],
    feeds: List[FeedConfig]
)

// ----- Logging & errors -----

enum ErrorKind:
  case Timeout, DnsFailure, ConnectionFailure, Http4xx, Http5xx, ParseError, Unexpected

// ----- HTTP results -----

final case class HeadResult(
    status: Option[Status],
    etag: Option[String],
    lastModified: Option[Instant],
    error: Option[ErrorKind],
    latency: FiniteDuration
)

final case class GetResult(
    status: Option[Status],
    body: Option[Array[Byte]],
    etag: Option[String],
    lastModified: Option[Instant],
    error: Option[ErrorKind],
    latency: FiniteDuration
)

// ----- Link state machine -----

enum LinkPhase:
  case NeedsInitialGet
  case NeedsHead
  case NeedsGet
  case Sleeping
  case ErrorBackoff

final case class LinkState(
    feedId: String,
    phase: LinkPhase,
    lastHeadAt: Option[Instant],
    lastHeadStatus: Option[Status],
    lastHeadError: Option[ErrorKind],
    lastGetAt: Option[Instant],
    lastGetStatus: Option[Status],
    lastGetError: Option[ErrorKind],
    etag: Option[String],
    lastModified: Option[Instant],
    backoffIndex: Int,
    basePollSeconds: Int,
    maxPollSeconds: Int,
    jitterFraction: Double,
    nextActionAt: Instant,
    jitterSeconds: Long,
    note: Option[String]
)

enum NextAction:
  case DoHead(state: LinkState)
  case DoGet(state: LinkState)
  case SleepUntil(at: Instant)

object LinkState:
  def initial(feed: FeedConfig, cfg: AppConfig, now: Instant): LinkState =
    LinkState(
      feedId = feed.id,
      phase = LinkPhase.NeedsInitialGet,
      lastHeadAt = None,
      lastHeadStatus = None,
      lastHeadError = None,
      lastGetAt = None,
      lastGetStatus = None,
      lastGetError = None,
      etag = None,
      lastModified = None,
      backoffIndex = 0,
      basePollSeconds = feed.basePollSeconds,
      maxPollSeconds = cfg.maxPollSeconds,
      jitterFraction = cfg.jitterFraction,
      nextActionAt = now,
      jitterSeconds = 0,
      note = Some("initial")
    )

  def decideNextAction(state: LinkState, now: Instant): NextAction =
    if now.isBefore(state.nextActionAt) then NextAction.SleepUntil(state.nextActionAt)
    else
      state.phase match
        case LinkPhase.NeedsInitialGet | LinkPhase.NeedsGet => NextAction.DoGet(state)
        case LinkPhase.NeedsHead                             => NextAction.DoHead(state)
        case LinkPhase.ErrorBackoff | LinkPhase.Sleeping     => NextAction.SleepUntil(state.nextActionAt)

  def applyHeadResult(
      state: LinkState,
      result: HeadResult,
      now: Instant,
      rand: Double
  ): LinkState =
    val modified = hasChanged(state, result.etag, result.lastModified, result.status)
    val isError = result.error.isDefined || result.status.exists(s => isErrorStatus(s))
    val (backoffIdx, phase, note) =
      if isError then (state.backoffIndex + 1, LinkPhase.ErrorBackoff, Some(s"head-error-${result.error}"))
      else if modified then (state.backoffIndex.max(0), LinkPhase.NeedsGet, Some("head-modified"))
      else (state.backoffIndex + 1, LinkPhase.Sleeping, Some("head-not-modified"))

    val delay = computeDelaySeconds(
      base = state.basePollSeconds,
      backoff = backoffIdx,
      maxSeconds = state.maxPollSeconds,
      jitterFraction = state.jitterFraction,
      rand = rand
    )
    state.copy(
      phase = phase,
      lastHeadAt = Some(now),
      lastHeadStatus = result.status,
      lastHeadError = result.error,
      backoffIndex = backoffIdx,
      etag = result.etag.orElse(state.etag),
      lastModified = result.lastModified.orElse(state.lastModified),
      nextActionAt = now.plusSeconds(delay.totalSeconds),
      jitterSeconds = delay.jitterSeconds,
      note = note
    )

  def applyGetResult(
      state: LinkState,
      result: GetResult,
      now: Instant,
      bodyChanged: Boolean,
      rand: Double
  ): LinkState =
    val isError = result.error.isDefined || result.status.exists(s => isErrorStatus(s))
    val (backoffIdx, phase, note) =
      if isError then (state.backoffIndex + 1, LinkPhase.ErrorBackoff, Some(s"get-error-${result.error}"))
      else if bodyChanged then (0, LinkPhase.Sleeping, Some("get-body-changed"))
      else (state.backoffIndex + 1, LinkPhase.Sleeping, Some("get-unchanged"))

    val delay = computeDelaySeconds(
      base = state.basePollSeconds,
      backoff = backoffIdx,
      maxSeconds = state.maxPollSeconds,
      jitterFraction = state.jitterFraction,
      rand = rand
    )
    state.copy(
      phase = if phase == LinkPhase.Sleeping then LinkPhase.NeedsHead else phase,
      lastGetAt = Some(now),
      lastGetStatus = result.status,
      lastGetError = result.error,
      etag = result.etag.orElse(state.etag),
      lastModified = result.lastModified.orElse(state.lastModified),
      backoffIndex = backoffIdx,
      nextActionAt = now.plusSeconds(delay.totalSeconds),
      jitterSeconds = delay.jitterSeconds,
      note = note
    )

  private def hasChanged(
      state: LinkState,
      etag: Option[String],
      lastModified: Option[Instant],
      status: Option[Status]
  ): Boolean =
    val byStatus = status.exists(s => s == Status.Ok && state.lastHeadStatus.contains(Status.NotModified))
    val byEtag = for
      a <- state.etag
      b <- etag
    yield a =!= b
    val byMod = for
      a <- state.lastModified
      b <- lastModified
    yield a != b
    byStatus || byEtag.contains(true) || byMod.contains(true)

  final case class Delay(totalSeconds: Long, jitterSeconds: Long)

  def computeDelaySeconds(
      base: Int,
      backoff: Int,
      maxSeconds: Int,
      jitterFraction: Double,
      rand: Double
  ): Delay =
    val baseSeconds = base.toLong * math.pow(2.0, backoff.toDouble).toLong
    val clamped = math.min(baseSeconds, maxSeconds.toLong)
    val jitterRaw = clamped.toDouble * jitterFraction
    val centered = (rand * 2.0 - 1.0) * jitterRaw
    val jitterSeconds = math.round(centered).toLong
    val total = math.max(0L, clamped + jitterSeconds)
    Delay(totalSeconds = total, jitterSeconds = jitterSeconds)

  private def isErrorStatus(s: Status): Boolean =
    s.responseClass match
      case Status.ResponseClass.ClientError | Status.ResponseClass.ServerError => true
      case _                                                                   => false

// ----- Helpers -----

object Hashing:
  def sha256(bytes: Array[Byte]): String =
    val md = MessageDigest.getInstance("SHA-256")
    md.digest(bytes).map("%02x".format(_)).mkString
