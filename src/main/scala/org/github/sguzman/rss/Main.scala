package org.github.sguzman.rss

import cats.effect.{ExitCode, IO, IOApp, Resource}
import cats.syntax.all._
import org.github.sguzman.rss.Logging.given
import org.github.sguzman.rss.config.ConfigLoader
import org.github.sguzman.rss.db.Database
import org.github.sguzman.rss.http.HttpClient
import org.github.sguzman.rss.runtime.Scheduler
import org.typelevel.log4cats.Logger

import java.nio.file.Path

object Main extends IOApp:
  def run(args: List[String]): IO[ExitCode] =
    val cfgPath = args.headOption.fold(Path.of("rss-config.toml"))(Path.of(_))
    val program = for
      cfg <- ConfigLoader.load[IO](cfgPath)
      _ <- Logger[IO].info(
        s"Loaded config for ${cfg.feeds.size} feeds, db=${cfg.dbPath}"
      )
      _ <- Database.transactor[IO](cfg).use { xa =>
        for
          _ <- Database.migrate(xa)
          _ <- Database.upsertFeeds(cfg.feeds, xa)
          _ <- Logger[IO].info(s"Domain limits: ${cfg.domains}")
          _ <- HttpClient.resource[IO](cfg).use { client =>
            Scheduler.loop[IO](cfg, client, xa).compile.drain
          }
        yield ()
      }
    yield ExitCode.Success

    program.handleErrorWith { err =>
      Logger[IO].error(err)(s"Fatal error: ${err.getMessage}") *> IO.pure(
        ExitCode.Error
      )
    }
