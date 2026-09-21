package com.jamesward.zio_bedrock.internal

import com.jamesward.zio_bedrock.Bedrock
import com.jamesward.zio_bedrock.Bedrock.{Error, StreamEvent}
import zio.{IO, Ref, ZIO}
import zio.http.Status
import zio.stream.ZStream

/** Backend-neutral normalized protocol consumed by the shared public API.
  * Backend implementations translate between this model and their wire format.
  */
private[zio_bedrock] trait Protocol:
  def send(request: Wire.ChatRequest): IO[Error, Wire.ChatResponse]

  /** Backend decoder output. Implementations must emit one final Complete. */
  def sendStreamEvents(request: Wire.ChatRequest): ZStream[Any, Error, StreamEvent]

  /** Reject truncated streams, duplicate completion, and events after completion. */
  final def completedStreamEvents(request: Wire.ChatRequest): ZStream[Any, Error, StreamEvent] =
    ZStream.unwrap:
      Ref.make(false).map: completed =>
        val checked = sendStreamEvents(request).mapZIO:
          case event @ Bedrock.StreamEvent.Complete(_) =>
            completed.getAndSet(true).flatMap: alreadyCompleted =>
              if alreadyCompleted then
                ZIO.fail(Error.Unexpected(Status.InternalServerError, "stream emitted more than one complete result"))
              else ZIO.succeed(event)
          case event =>
            completed.get.flatMap: alreadyCompleted =>
              if alreadyCompleted then
                ZIO.fail(Error.Unexpected(Status.InternalServerError, "stream emitted an event after its complete result"))
              else ZIO.succeed(event)
        val requireCompletion = ZStream.fromZIO:
          completed.get.flatMap: didComplete =>
            if didComplete then ZIO.unit
            else ZIO.fail(Error.Unexpected(Status.InternalServerError, "stream ended without a complete result"))
        checked ++ requireCompletion.drain

  final def sendStream(request: Wire.ChatRequest): ZStream[Any, Error, String] =
    completedStreamEvents(request).collect:
      case Bedrock.StreamEvent.TextDelta(text) => text
