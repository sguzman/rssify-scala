package org.github.sguzman.rss.http

import cats.effect.kernel.Async
import cats.syntax.all.*
import org.github.sguzman.rss.model.*
import org.http4s.client.Client
import org.http4s.ember.client.EmberClientBuilder
import org.http4s.headers.{ETag, `Last-Modified`}
import org.http4s.{Header, Headers, Method, Request, Uri}
import org.typelevel.ci.CIString

import scala.concurrent.duration.*
import scala.annotation.nowarn

object HttpClient:
  @nowarn("cat=deprecation")
  def resource[F[_]: Async]: cats.effect.Resource[
    F,
    Client[F]
  ] =
    EmberClientBuilder
      .default[F]
      .withIdleTimeInPool(2.minutes)
      .withTimeout(30.seconds)
      .build

  private def headerUserAgent(
      ua: String
  ): Headers =
    Headers(
      Header.Raw(
        CIString("User-Agent"),
        ua
      )
    )

  def doHead[F[_]: Async](
      client: Client[F],
      uri: Uri,
      ua: String
  ): F[HeadResult] =
    val request = Request[F](
      Method.HEAD,
      uri,
      headers = headerUserAgent(ua)
    )
    timed {
      client.run(request).use { resp =>
        val etag = resp.headers
          .get[ETag]
          .map(_.tag.toString)
        val lastModified = resp.headers
          .get[`Last-Modified`]
          .map(_.date.toInstant)
        val status = resp.status
        HeadResult(
          status = Some(status),
          etag = etag,
          lastModified = lastModified,
          error = None,
          latency = 0.millis
        ).pure[F]
      }
    }.handleErrorWith(t =>
      headError(t).pure[F]
    )

  def doGet[F[_]: Async](
      client: Client[F],
      uri: Uri,
      ua: String
  ): F[GetResult] =
    val request = Request[F](
      Method.GET,
      uri,
      headers = headerUserAgent(ua)
    )
    timed {
      client.run(request).use { resp =>
        for
          body <- resp.body.compile.to(
            Array
          )
          etag = resp.headers
            .get[ETag]
            .map(_.tag.toString)
          lastModified = resp.headers
            .get[`Last-Modified`]
            .map(_.date.toInstant)
        yield GetResult(
          status = Some(resp.status),
          body = Some(body),
          etag = etag,
          lastModified = lastModified,
          error = None,
          latency = 0.millis
        )
      }
    }.handleErrorWith(t =>
      getError(t).pure[F]
    )

  private def timed[F[_]: Async, A](
      fa: F[A]
  )(using ev: LatencyLens[A]): F[A] =
    for
      start <- Async[F].monotonic
      a <- fa
      end <- Async[F].monotonic
      latency = end - start
    yield ev.setLatency(a, latency)

  trait LatencyLens[A]:
    def setLatency(
        a: A,
        lat: FiniteDuration
    ): A

  given LatencyLens[HeadResult] with
    def setLatency(
        a: HeadResult,
        lat: FiniteDuration
    ): HeadResult =
      a.copy(latency = lat)

  given LatencyLens[GetResult] with
    def setLatency(
        a: GetResult,
        lat: FiniteDuration
    ): GetResult = a.copy(latency = lat)

  private def classifyError(
      t: Throwable
  ): ErrorKind =
    val msg = Option(t.getMessage)
      .map(_.toLowerCase)
      .getOrElse("")
    if msg.contains("timeout") then ErrorKind.Timeout
    else if msg.contains("unknown host") || msg
        .contains("dns")
    then ErrorKind.DnsFailure
    else if msg.contains("connection")
    then ErrorKind.ConnectionFailure
    else ErrorKind.Unexpected

  private def headError(
      t: Throwable
  ): HeadResult =
    HeadResult(
      status = None,
      etag = None,
      lastModified = None,
      error = Some(classifyError(t)),
      latency = 0.millis
    )

  private def getError(
      t: Throwable
  ): GetResult =
    GetResult(
      status = None,
      body = None,
      etag = None,
      lastModified = None,
      error = Some(classifyError(t)),
      latency = 0.millis
    )
