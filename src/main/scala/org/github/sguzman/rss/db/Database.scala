package org.github.sguzman.rss.db

import cats.effect.*
import cats.syntax.all.*
import doobie.*
import doobie.implicits.*
import doobie.hikari.HikariTransactor
import doobie.util.transactor.Transactor
import doobie.util.meta.Meta
import org.github.sguzman.rss.model.*
import org.github.sguzman.rss.feed.FeedParser
import org.github.sguzman.rss.time.Time
import org.typelevel.log4cats.Logger

import java.sql.Timestamp
import java.time.{Instant, ZoneId}

given Meta[Instant] =
  Meta[Timestamp].imap(_.toInstant)(
    Timestamp.from
  )

private def asText(
    instant: Instant,
    zone: ZoneId
): String =
  Time.instantToDbString(
    instant,
    zone
  )

private def asTextOpt(
    instant: Option[Instant],
    zone: ZoneId
): Option[String] =
  instant.map(asText(_, zone))

object Database:
  def transactor[F[_]: Async](
      cfg: AppConfig
  ): Resource[F, HikariTransactor[F]] =
    for
      ce <- ExecutionContexts
        .fixedThreadPool[F](4)
      xa <- HikariTransactor
        .newHikariTransactor[F](
          driverClassName = "org.sqlite.JDBC",
          url = s"jdbc:sqlite:${cfg.dbPath.toString}",
          user = "",
          pass = "",
          connectEC = ce
        )
    yield xa

  def migrate[F[_]: Async](
      xa: Transactor[F]
  ): F[Unit] =
    val ddl = List(
      sql"""
        CREATE TABLE IF NOT EXISTS feeds(
          id TEXT PRIMARY KEY,
          url TEXT NOT NULL,
          domain TEXT NOT NULL,
          created_at TIMESTAMP NOT NULL,
          created_at_text TEXT NOT NULL
        )
      """.update.run,
      sql"""
        CREATE TABLE IF NOT EXISTS feed_state_history(
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          feed_id TEXT NOT NULL REFERENCES feeds(id),
          recorded_at TIMESTAMP NOT NULL,
          recorded_at_text TEXT NOT NULL,
          phase TEXT NOT NULL,
          last_head_at TIMESTAMP NULL,
          last_head_at_text TEXT NULL,
          last_head_status INTEGER NULL,
          last_head_error TEXT NULL,
          last_get_at TIMESTAMP NULL,
          last_get_at_text TEXT NULL,
          last_get_status INTEGER NULL,
          last_get_error TEXT NULL,
          etag TEXT NULL,
          last_modified TIMESTAMP NULL,
          last_modified_text TEXT NULL,
          backoff_index INTEGER NOT NULL,
          base_poll_seconds INTEGER NOT NULL,
          next_action_at TIMESTAMP NOT NULL,
          next_action_at_text TEXT NOT NULL,
          jitter_seconds INTEGER NOT NULL,
          note TEXT NULL
        )
      """.update.run,
      sql"""
        CREATE TABLE IF NOT EXISTS fetch_events(
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          feed_id TEXT NOT NULL REFERENCES feeds(id),
          event_time TIMESTAMP NOT NULL,
          event_time_text TEXT NOT NULL,
          method TEXT NOT NULL,
          status INTEGER NULL,
          error_kind TEXT NULL,
          latency_ms INTEGER NULL,
          backoff_index INTEGER NOT NULL,
          scheduled_next_action_at TIMESTAMP NOT NULL,
          scheduled_next_action_at_text TEXT NOT NULL,
          debug TEXT NULL
        )
      """.update.run,
      sql"""
        CREATE TABLE IF NOT EXISTS feed_payloads(
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          feed_id TEXT NOT NULL REFERENCES feeds(id),
          fetched_at TIMESTAMP NOT NULL,
          fetched_at_text TEXT NOT NULL,
          etag TEXT NULL,
          last_modified TIMESTAMP NULL,
          last_modified_text TEXT NULL,
          content_hash TEXT NULL,
          title TEXT NULL,
          link TEXT NULL,
          description TEXT NULL,
          language TEXT NULL,
          updated_at TIMESTAMP NULL,
          updated_at_text TEXT NULL
        )
      """.update.run,
      sql"""
        CREATE TABLE IF NOT EXISTS feed_items(
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          payload_id INTEGER NOT NULL REFERENCES feed_payloads(id) ON DELETE CASCADE,
          feed_id TEXT NOT NULL REFERENCES feeds(id),
          title TEXT NULL,
          link TEXT NULL,
          guid TEXT NULL,
          author TEXT NULL,
          published_at TIMESTAMP NULL,
          published_at_text TEXT NULL,
          updated_at TIMESTAMP NULL,
          updated_at_text TEXT NULL,
          summary TEXT NULL,
          content TEXT NULL
        )
      """.update.run,
      sql"DROP TABLE IF EXISTS feed_bodies".update.run
    )
    ddl.sequence_.transact(xa).void

  def upsertFeeds[F[_]: Async: Logger](
      feeds: List[FeedConfig],
      zone: ZoneId,
      xa: Transactor[F]
  ): F[Unit] =
    feeds.traverse_ { feed =>
      val now = Instant.now()
      sql"""
        INSERT OR IGNORE INTO feeds(
          id, url, domain, created_at, created_at_text
        )
        VALUES (
          ${feed.id},
          ${feed.url.toString},
          ${feed.domain},
          $now,
          ${asText(now, zone)}
        )
      """.update.run.transact(xa).void
    }

  final case class StateRow(
      feedId: String,
      phase: String,
      lastHeadAt: Option[Instant],
      lastHeadStatus: Option[Int],
      lastHeadError: Option[String],
      lastGetAt: Option[Instant],
      lastGetStatus: Option[Int],
      lastGetError: Option[String],
      etag: Option[String],
      lastModified: Option[Instant],
      backoffIndex: Int,
      basePollSeconds: Int,
      nextActionAt: Instant,
      jitterSeconds: Long,
      note: Option[String]
  )

  def insertState[F[_]: Async](
      state: LinkState,
      recordedAt: Instant,
      zone: ZoneId,
      xa: Transactor[F]
  ): F[Unit] =
    sql"""
      INSERT INTO feed_state_history(
        feed_id, recorded_at, recorded_at_text, phase,
        last_head_at, last_head_at_text, last_head_status, last_head_error,
        last_get_at, last_get_at_text, last_get_status, last_get_error,
        etag, last_modified, last_modified_text,
        backoff_index, base_poll_seconds, next_action_at, next_action_at_text,
        jitter_seconds, note
      ) VALUES (
        ${state.feedId},
        $recordedAt,
        ${asText(recordedAt, zone)},
        ${state.phase.toString},
        ${state.lastHeadAt},
        ${asTextOpt(state.lastHeadAt, zone)},
        ${state.lastHeadStatus.map(
        _.code
      )}, ${state.lastHeadError.map(
        _.toString
      )},
        ${state.lastGetAt},
        ${asTextOpt(state.lastGetAt, zone)},
        ${state.lastGetStatus
        .map(
          _.code
        )}, ${state.lastGetError.map(
        _.toString
      )},
        ${state.etag},
        ${state.lastModified},
        ${asTextOpt(state.lastModified, zone)},
        ${state.backoffIndex},
        ${state.basePollSeconds},
        ${state.nextActionAt},
        ${asText(state.nextActionAt, zone)},
        ${state.jitterSeconds},
        ${state.note}
      )
    """.update.run.transact(xa).void

  def insertEvent[F[_]: Async](
      feedId: String,
      method: String,
      status: Option[Int],
      errorKind: Option[ErrorKind],
      latency: Option[Long],
      backoffIndex: Int,
      scheduled: Instant,
      debug: Option[String],
      zone: ZoneId,
      xa: Transactor[F]
  ): F[Unit] =
    val now = Instant.now()
    sql"""
      INSERT INTO fetch_events(
        feed_id, event_time, event_time_text, method,
        status, error_kind, latency_ms, backoff_index,
        scheduled_next_action_at, scheduled_next_action_at_text, debug
      ) VALUES (
        $feedId,
        $now,
        ${asText(now, zone)},
        $method,
        $status,
        ${errorKind
        .map(
          _.toString
        )},
        $latency,
        $backoffIndex,
        $scheduled,
        ${asText(scheduled, zone)},
        $debug
      )
    """.update.run.transact(xa).void

  def insertPayloadWithItems[F[_]: Async](
      feedId: String,
      fetchedAt: Instant,
      etag: Option[String],
      lastModified: Option[Instant],
      contentHash: Option[String],
      parsed: FeedParser.ParsedFeed,
      zone: ZoneId,
      xa: Transactor[F]
  ): F[Unit] =
    val insertPayload: ConnectionIO[Long] =
      sql"""
        INSERT INTO feed_payloads(
          feed_id, fetched_at, fetched_at_text, etag,
          last_modified, last_modified_text, content_hash,
          title, link, description, language,
          updated_at, updated_at_text
        ) VALUES (
          $feedId,
          $fetchedAt,
          ${asText(fetchedAt, zone)},
          $etag,
          $lastModified,
          ${asTextOpt(lastModified, zone)},
          $contentHash,
          ${parsed.metadata.title},
          ${parsed.metadata.link},
          ${parsed.metadata.description},
          ${parsed.metadata.language},
          ${parsed.metadata.updatedAt},
          ${asTextOpt(parsed.metadata.updatedAt, zone)}
        )
      """.update
        .withUniqueGeneratedKeys[Long]("id")

    val insertItems: Long => ConnectionIO[Unit] =
      payloadId =>
        parsed.items.traverse_ { item =>
          sql"""
            INSERT INTO feed_items(
              payload_id, feed_id, title, link, guid, author,
              published_at, published_at_text,
              updated_at, updated_at_text,
              summary, content
            ) VALUES (
              $payloadId,
              $feedId,
              ${item.title},
              ${item.link},
              ${item.guid},
              ${item.author},
              ${item.publishedAt},
              ${asTextOpt(item.publishedAt, zone)},
              ${item.updatedAt},
              ${asTextOpt(item.updatedAt, zone)},
              ${item.summary},
              ${item.content}
            )
          """.update.run.void
        }

    (for
      payloadId <- insertPayload
      _ <- insertItems(payloadId)
    yield ()).transact(xa)

  def latestState[F[_]: Async](
      feedId: String,
      xa: Transactor[F]
  ): F[Option[StateRow]] =
    sql"""
      SELECT feed_id, phase, last_head_at, last_head_status, last_head_error, last_get_at,
             last_get_status, last_get_error, etag, last_modified, backoff_index, base_poll_seconds,
             next_action_at, jitter_seconds, note
      FROM feed_state_history
      WHERE feed_id = $feedId
      ORDER BY id DESC
      LIMIT 1
    """
      .query[StateRow]
      .option
      .transact(xa)

  def dueFeeds[F[_]: Async](
      now: Instant,
      feeds: List[FeedConfig],
      xa: Transactor[F]
  ): F[List[FeedConfig]] =
    feeds.filterA { feed =>
      latestState(feed.id, xa).map {
        case None => true
        case Some(row)
            if row.nextActionAt
              .isBefore(
                now
              ) || row.nextActionAt
              .equals(now) =>
          true
        case _ => false
      }
    }
