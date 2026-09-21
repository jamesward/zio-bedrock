package com.jamesward.zio_bedrock

import com.jamesward.zio_bedrock.Bedrock.{Error, Metrics, Role, StopReason, TokenUsage, ToolName, ToolUseId}
import com.jamesward.zio_bedrock.internal.Codecs.given
import com.jamesward.zio_bedrock.internal.{Codecs, Protocol, Wire}
import zio.*
import zio.json.*
import zio.json.ast.Json
import zio.schema.Schema
import zio.schema.codec.json.schemaJson
import zio.schema.codec.JsonCodec
import zio.stream.*

import scala.collection.immutable.Queue

/**
 * Test-side mock for `Bedrock`. Scripts the model's behaviour as
 * a queue of [[MockBehavior]]s; each `send` consumes one behavior and
 * builds the matching wire response.
 *
 * Lives in `src/test` so the production jar carries no test-only code.
 */
object BedrockMock:

  /** A scripted model behavior. Each `send` round consumes one. */
  sealed trait MockBehavior

  object MockBehavior:
    /** The model produces a final text response (`stopReason = stop`). */
    case class Reply(text: String) extends MockBehavior

    /** The model produces a JSON response that decodes to `value` —
      * used for structured-output tests. */
    case class ReplyJson[T: Schema](value: T) extends MockBehavior:
      val schema: Schema[T] = summon[Schema[T]]

    /** The model invokes a tool after emitting same-turn commentary. */
    case class CallToolWithText[I: Schema](text: String, toolName: ToolName, input: I) extends MockBehavior:
      val schema: Schema[I] = summon[Schema[I]]

    /** The model invokes a tool. Surfaces `stopReason = tool_calls`. */
    case class CallTool[I: Schema](toolName: ToolName, input: I) extends MockBehavior:
      val schema: Schema[I] = summon[Schema[I]]

    /** The model fails the chat-completion call with the given error. */
    case class Fail(error: Error) extends MockBehavior

  /** A `Bedrock` layer whose responses come from `behaviors`. */
  def apply(behaviors: MockBehavior*): ULayer[Bedrock] =
    ZLayer.fromZIO:
      Ref.make(Queue.from(behaviors.toIndexedSeq)).map: ref =>
        new Bedrock:
          private[zio_bedrock] val protocol: Protocol = new Protocol:
            def send(req: Wire.ChatRequest): IO[Error, Wire.ChatResponse] =
              respond(ref)
            def sendStreamEvents(req: Wire.ChatRequest): ZStream[Any, Error, Bedrock.StreamEvent] =
              ZStream.fromZIO(respond(ref)).flatMap: wire =>
                val contentEvents = wire.output.message.content.zipWithIndex.flatMap:
                  case (Wire.ContentBlock.Text(text), index) =>
                    List(Bedrock.StreamEvent.TextDelta(text), Bedrock.StreamEvent.ContentBlockStop(index))
                  case (Wire.ContentBlock.ToolUse(value), index) =>
                    val input = summon[Schema[Json]].fromDynamic(value.input).getOrElse(Json.Obj()).toJson
                    List(
                      Bedrock.StreamEvent.ToolUseStart(value.toolUseId, value.name),
                      Bedrock.StreamEvent.ToolUseDelta(value.toolUseId, input),
                      Bedrock.StreamEvent.ContentBlockStop(index),
                    )
                  case _ => Nil
                val stopEvent = Bedrock.StreamEvent.MessageStop(wire.stopReason)
                val metaEvent = Bedrock.StreamEvent.Metadata(wire.usage, wire.metrics)
                val completeEvent = Bedrock.StreamEvent.Complete(Bedrock.StreamComplete.fromWire(wire))
                ZStream.fromIterable(contentEvents :+ stopEvent :+ metaEvent :+ completeEvent)

  private def respond(ref: Ref[Queue[MockBehavior]]):
      IO[Error, Wire.ChatResponse] =
    ref.modify:
      case q if q.isEmpty => (None, q)
      case q              =>
        val (head, rest) = q.dequeue
        (Some(head), rest)
    .flatMap:
      case None => ZIO.fail(Error.Unexpected(
        zio.http.Status.InternalServerError,
        "Mock script exhausted: more `send` rounds than scripted behaviors",
      ))
      case Some(MockBehavior.Reply(text)) =>
        ZIO.succeed(wrap(List(Wire.ContentBlock.Text(text)), StopReason.EndTurn))
      case Some(b: MockBehavior.ReplyJson[t]) =>
        val codec = JsonCodec.schemaBasedBinaryCodec[t](Codecs.codecConfig)(using b.schema)
        val json  = new String(codec.encode(b.value).toArray, java.nio.charset.StandardCharsets.UTF_8)
        ZIO.succeed(wrap(List(Wire.ContentBlock.Text(json)), StopReason.EndTurn))
      case Some(b: MockBehavior.CallToolWithText[i]) =>
        val tu = Wire.ToolUseContent(
          toolUseId = ToolUseId(java.util.UUID.randomUUID().toString),
          name      = b.toolName,
          input     = b.schema.toDynamic(b.input),
        )
        ZIO.succeed(wrap(List(Wire.ContentBlock.Text(b.text), Wire.ContentBlock.ToolUse(tu)), StopReason.ToolUse))
      case Some(b: MockBehavior.CallTool[i]) =>
        val tu = Wire.ToolUseContent(
          toolUseId = ToolUseId(java.util.UUID.randomUUID().toString),
          name      = b.toolName,
          input     = b.schema.toDynamic(b.input),
        )
        ZIO.succeed(wrap(List(Wire.ContentBlock.ToolUse(tu)), StopReason.ToolUse))
      case Some(MockBehavior.Fail(error)) =>
        ZIO.fail(error)

  private def wrap(
    content:    List[Wire.ContentBlock],
    stopReason: StopReason,
  ): Wire.ChatResponse =
    Wire.ChatResponse(
      output     = Wire.ChatOutput(Wire.WireMessage(
        role    = Role.Assistant,
        content = content,
      )),
      stopReason = stopReason,
      usage      = TokenUsage(inputTokens = 0, outputTokens = 0, totalTokens = 0),
      metrics    = Metrics(),
    )
