package org.github.sguzman.rss.db

import cats.effect.*
import cats.syntax.all.*
import doobie.*
import doobie.implicits.*
import doobie.hikari.HikariTransactor
import doobie.util.transactor.Transactor
import org.github.sguzman.rss.model.*
import org.typelevel.log4cats.Logger

import java.time.Instant

object Database:
  def transactor[F[_]: Async](cfg: AppConfig): Resource[F, HikariTransactor[F]] =
    for
      ce <- ExecutionContexts.fixedThreadPool[F](4)
      xa <- HikariTransactor.newHikariTransactor[F](
        driverClassName = "org.sqlite.JDBC",
        url = s"jdbc:sqlite:${cfg.dbPath.toString}",
        user = "",
        pass = "",
        connectEC = ce
      )
    yield xa

  def migrate[F[_]: Async](xa: Transactor[F]): F[Unit] =
    val ddl = List(
      sql"""
        CREATE TABLE IF NOT EXISTS feeds(
          id TEXT PRIMARY KEY,
          url TEXT NOT NULL,
          domain TEXT NOT NULL,
          created_at TIMESTAMP NOT NULL
        )
      """.update.run,
      sql"""
        CREATE TABLE IF NOT EXISTS feed_state_history(
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          feed_id TEXT NOT NULL REFERENCES feeds(id),
          recorded_at TIMESTAMP NOT NULL,
          phase TEXT NOT NULL,
          last_head_at TIMESTAMP NULL,
          last_head_status INTEGER NULL,
          last_head_error TEXT NULL,
          last_get_at TIMESTAMP NULL,
          last_get_status INTEGER NULL,
          last_get_error TEXT NULL,
          etag TEXT NULL,
          last_modified TIMESTAMP NULL,
          backoff_index INTEGER NOT NULL,
          base_poll_seconds INTEGER NOT NULL,
          next_action_at TIMESTAMP NOT NULL,
          jitter_seconds INTEGER NOT NULL,
          note TEXT NULL
        )
      """.update.run,
      sql"""
        CREATE TABLE IF NOT EXISTS fetch_events(
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          feed_id TEXT NOT NULL REFERENCES feeds(id),
          event_time TIMESTAMP NOT NULL,
          method TEXT NOT NULL,
          status INTEGER NULL,
          error_kind TEXT NULL,
          latency_ms INTEGER NULL,
          backoff_index INTEGER NOT NULL,
          scheduled_next_action_at TIMESTAMP NOT NULL,
          debug TEXT NULL
        )
      """.update.run,
      sql"""
        CREATE TABLE IF NOT EXISTS feed_bodies(
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          feed_id TEXT NOT NULL REFERENCES feeds(id),
          fetched_at TIMESTAMP NOT NULL,
          etag TEXT NULL,
          last_modified TIMESTAMP NULL,
          content_hash TEXT NULL,
          body BLOB NOT NULL
        )
      """.update.run
    )
    ddl.sequence_.transact(xa).void

  def upsertFeeds[F[_]: Async: Logger](feeds: List[FeedConfig], xa: Transactor[F]): F[Unit] =
    feeds.traverse_ { feed =>
      sql"""
        INSERT OR IGNORE INTO feeds(id, url, domain, created_at)
        VALUES (${feed.id}, ${feed.url.toString}, ${feed.domain}, ${Instant.now()})
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
      xa: Transactor[F]
  ): F[Unit] =
    sql"""
      INSERT INTO feed_state_history(
        feed_id, recorded_at, phase, last_head_at, last_head_status, last_head_error,
        last_get_at, last_get_status, last_get_error, etag, last_modified, backoff_index,
        base_poll_seconds, next_action_at, jitter_seconds, note
      ) VALUES (
        ${state.feedId}, $recordedAt, ${state.phase.toString}, ${state.lastHeadAt},
        ${state.lastHeadStatus.map(_.code)}, ${state.lastHeadError.map(_.toString)},
        ${state.lastGetAt}, ${state.lastGetStatus.map(_.code)}, ${state.lastGetError.map(_.toString)},
        ${state.etag}, ${state.lastModified}, ${state.backoffIndex}, ${state.basePollSeconds},
        ${state.nextActionAt}, ${state.jitterSeconds}, ${state.note}
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
      xa: Transactor[F]
  ): F[Unit] =
    sql"""
      INSERT INTO fetch_events(
        feed_id, event_time, method, status, error_kind, latency_ms, backoff_index, scheduled_next_action_at, debug
      ) VALUES (
        $feedId, ${Instant.now()}, $method, $status, ${errorKind.map(_.toString)}, $latency, $backoffIndex, $scheduled, $debug
      )
    """.update.run.transact(xa).void

  def insertBody[F[_]: Async](
      feedId: String,
      fetchedAt: Instant,
      etag: Option[String],
      lastModified: Option[Instant],
      contentHash: Option[String],
      body: Array[Byte],
      xa: Transactor[F]
  ): F[Unit] =
    sql"""
      INSERT INTO feed_bodies(feed_id, fetched_at, etag, last_modified, content_hash, body)
      VALUES ($feedId, $fetchedAt, $etag, $lastModified, $contentHash, $body)
    """.update.run.transact(xa).void

  def latestState[F[_]: Async](feedId: String, xa: Transactor[F]): F[Option[StateRow]] =
    sql"""
      SELECT feed_id, phase, last_head_at, last_head_status, last_head_error, last_get_at,
             last_get_status, last_get_error, etag, last_modified, backoff_index, base_poll_seconds,
             next_action_at, jitter_seconds, note
      FROM feed_state_history
      WHERE feed_id = $feedId
      ORDER BY id DESC
      LIMIT 1
    """.query[StateRow].option.transact(xa)

  def dueFeeds[F[_]: Async](
      now: Instant,
      feeds: List[FeedConfig],
      xa: Transactor[F]
  ): F[List[FeedConfig]] =
    feeds.filterA { feed =>
      latestState(feed.id, xa).map {
        case None                        => true
        case Some(row) if row.nextActionAt.isBefore(now) || row.nextActionAt.equals(now) => true
        case _                           => false
      }
    }
