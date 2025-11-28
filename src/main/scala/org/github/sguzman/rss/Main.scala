package org.github.sguzman.rss

import cats.effect.{ExitCode, IO, IOApp}
import org.github.sguzman.rss.Logging.given
import org.github.sguzman.rss.config.ConfigLoader
import org.github.sguzman.rss.db.Database
import org.github.sguzman.rss.http.HttpClient
import org.github.sguzman.rss.model.AppMode
import org.github.sguzman.rss.runtime.Scheduler
import org.github.sguzman.rss.time.Time
import org.typelevel.log4cats.Logger

object Main extends IOApp {
  def run(
      args: List[String]
  ): IO[ExitCode] = {
    val defaultConfigPath =
      os.pwd / "src" / "main" / "resources" / "config" / "config.toml"
    val cfgPath = args.headOption
      .map(p => os.Path(p, base = os.pwd))
      .getOrElse(defaultConfigPath)
    val program = for
      cfg <- ConfigLoader.load[IO](
        cfgPath
      )
      _ <- IO(
        Time.setDefaultTimezone(
          cfg.timezone
        )
      )
      _ <- Logger[IO].info(
        s"Using timezone ${cfg.timezone}"
      )
      _ <- resetDbIfDev(cfg)
      _ <- Logger[IO].info(
        s"Loaded config for ${cfg.feeds.size} feeds, db=${cfg.dbPath}, mode=${cfg.mode}"
      )
      _ <- Database
        .transactor[IO](cfg)
        .use { xa =>
          for
            _ <- Database.migrate(xa)
            _ <- Database.upsertFeeds(
              cfg.feeds,
              cfg.timezone,
              xa
            )
            _ <- Logger[IO].info(
              s"Domain limits: ${cfg.domains}"
            )
            _ <- HttpClient
              .resource[IO]
              .use { client =>
                Scheduler
                  .loop[IO](
                    cfg,
                    client,
                    xa
                  )
                  .compile
                  .drain
              }
          yield ()
        }
    yield ExitCode.Success

    program.handleErrorWith { err =>
      Logger[IO].error(err)(
        s"Fatal error: ${err.getMessage}"
      ) *> IO.pure(
        ExitCode.Error
      )
    }
  }

  private def resetDbIfDev(
      cfg: org.github.sguzman.rss.model.AppConfig
  ): IO[Unit] =
    cfg.mode match
      case AppMode.Dev =>
        val dbOsPath =
          os.Path(cfg.dbPath.toString)
        Logger[IO].warn(
          s"Dev mode enabled, deleting database at ${cfg.dbPath}"
        ) *>
          IO.blocking {
            if os.exists(dbOsPath) then os.remove(dbOsPath)
          }.void
      case AppMode.Prod => IO.unit
}
