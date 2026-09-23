package com.jamesward.zio_bedrock

import com.jamesward.zio_bedrock.Bedrock.*
import com.jamesward.zio_bedrock.internal.Codecs.given
import com.jamesward.zio_bedrock.internal.{Codecs, Protocol, Wire}
import zio.*
import zio.json.*
import zio.json.ast.Json
import zio.schema.codec.JsonCodec
import zio.schema.codec.json.schemaJson
import zio.stream.ZStream

/** Composable middleware around normalized Bedrock protocol exchanges.
  * Composition follows ZIO HTTP: `left ++ right` applies `left` around
  * `right`, and `@@` is an alias for `++`.
  */
trait BedrockMiddleware { self =>
  import BedrockMiddleware.*

  def apply(request: Request, next: IO[Error, Response]): IO[Error, Response]

  def applyStream(
    request: Request,
    next: ZStream[Any, Error, StreamEvent],
  ): ZStream[Any, Error, StreamEvent]

  final def @@(that: BedrockMiddleware): BedrockMiddleware =
    self ++ that

  final def ++(that: BedrockMiddleware): BedrockMiddleware =
    BedrockMiddleware.make(
      unary = (request, next) => self(request, that(request, next)),
      streaming = (request, next) => self.applyStream(request, that.applyStream(request, next)),
    )

  final def layer: URLayer[Bedrock, Bedrock] =
    ZLayer.fromFunction: (underlying: Bedrock) =>
      new Bedrock:
        private[zio_bedrock] val protocol: Protocol = new Protocol:
          def send(request: Wire.ChatRequest): IO[Error, Wire.ChatResponse] =
            self(
              Request(Operation.Unary, encode(request)),
              underlying.protocol.send(request).map(Response(_)),
            ).map(_.wire)

          def sendStreamEvents(request: Wire.ChatRequest): ZStream[Any, Error, StreamEvent] =
            self.applyStream(
              Request(Operation.Streaming, encode(request)),
              underlying.protocol.sendStreamEvents(request),
            )
}

object BedrockMiddleware:
  enum Operation:
    case Unary, Streaming

  final case class Request(operation: Operation, body: Json)

  final class Response private[zio_bedrock] (
    private[zio_bedrock] val wire: Wire.ChatResponse,
  ):
    lazy val body: Json = encode(wire)

  private[zio_bedrock] object Response:
    def apply(wire: Wire.ChatResponse): Response = new Response(wire)

  final case class LoggingConfig(
    logRequest: Boolean = true,
    logResponse: Boolean = true,
  )

  val identity: BedrockMiddleware = make(
    unary = (_, next) => next,
    streaming = (_, next) => next,
  )

  def make(
    unary: (Request, IO[Error, Response]) => IO[Error, Response],
    streaming: (Request, ZStream[Any, Error, StreamEvent]) => ZStream[Any, Error, StreamEvent] = (_, next) => next,
  ): BedrockMiddleware =
    new BedrockMiddleware:
      def apply(request: Request, next: IO[Error, Response]): IO[Error, Response] =
        unary(request, next)

      def applyStream(
        request: Request,
        next: ZStream[Any, Error, StreamEvent],
      ): ZStream[Any, Error, StreamEvent] =
        streaming(request, next)

  /** Runs an effect after each unary exchange without allowing callback
    * defects or self-interruption to change the Bedrock result.
    */
  def observe(
    f: (Request, Exit[Error, Response], Long) => UIO[Unit]
  ): BedrockMiddleware =
    make(
      unary = (request, next) =>
        Clock.nanoTime.flatMap: started =>
          next.onExit: exit =>
            Clock.nanoTime.flatMap: finished =>
              notifyBestEffort:
                f(request, exit, (finished - started) / 1000000L),
    )

  /** Logs complete normalized requests and canonical unary responses. Streaming
    * requests are logged once, followed by terminal completion or failure.
    * Enable only where logs may contain prompts, tool input, and model output.
    */
  def logging(config: LoggingConfig = LoggingConfig()): BedrockMiddleware =
    make(
      unary = observeUnary(config),
      streaming = (request, next) =>
        val requestLog =
          if config.logRequest then ZIO.logInfo(s"Bedrock stream started; request=${request.body.toJson}")
          else ZIO.unit
        ZStream.fromZIO(requestLog).drain ++
          next.tap:
            case StreamEvent.Complete(complete) if config.logResponse =>
              ZIO.logInfo(s"Bedrock stream completed; response=$complete")
            case _ => ZIO.unit
          .tapErrorCause(cause => ZIO.logErrorCause("Bedrock stream failed", cause)),
    )

  private def observeUnary(
    config: LoggingConfig
  ): (Request, IO[Error, Response]) => IO[Error, Response] =
    (request, next) =>
      Clock.nanoTime.flatMap: started =>
        next.onExit: exit =>
          Clock.nanoTime.flatMap: finished =>
            val latencyMs = (finished - started) / 1000000L
            notifyBestEffort:
              exit match
                case Exit.Success(response) =>
                  val requestText = if config.logRequest then s"; request=${request.body.toJson}" else ""
                  val responseText = if config.logResponse then s"; response=${response.body.toJson}" else ""
                  ZIO.logInfo(s"Bedrock exchange completed in ${latencyMs}ms$requestText$responseText")
                case Exit.Failure(cause) =>
                  val requestText = if config.logRequest then s"; request=${request.body.toJson}" else ""
                  ZIO.logErrorCause(s"Bedrock exchange failed in ${latencyMs}ms$requestText", cause)

  private def notifyBestEffort(effect: => UIO[Any]): UIO[Unit] =
    ZIO.suspendSucceed(effect).unit
      .catchAllCause(cause =>
        ZIO.logErrorCause("Bedrock middleware callback failed", cause)
          .catchAllCause(_ => ZIO.unit)
      )
      .uninterruptible

  private[zio_bedrock] def encode[A: zio.schema.Schema](value: A): Json =
    JsonCodec.schemaBasedBinaryCodec[A](Codecs.codecConfig)
      .encode(value)
      .asString
      .fromJson[Json]
      .fold(error => throw IllegalStateException(s"Could not encode middleware payload: $error"), value => value)

  extension [R, E](layer: ZLayer[R, E, Bedrock])
    def @@(middleware: BedrockMiddleware): ZLayer[R, E, Bedrock] =
      layer >>> middleware.layer
