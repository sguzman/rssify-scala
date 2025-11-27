package org.github.sguzman.rss

import cats.effect.kernel.Sync
import org.typelevel.log4cats.Logger
import org.typelevel.log4cats.slf4j.Slf4jLogger

object Logging:
  given [F[_]: Sync]: Logger[F] =
    Slf4jLogger.getLogger[F]
