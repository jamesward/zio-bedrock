package com.jamesward.zio_bedrock

import com.jamesward.zio_bedrock.Bedrock.*
import zio.*
import zio.schema.{Schema, derived}
import zio.stream.ZStream
import zio.test.*

object MiddlewareSpec extends ZIOSpecDefault:
  private final case class StructuredReply(value: String) derives Schema, CanEqual

  def spec = suite("Bedrock middleware")(
    test("composes left around right like ZIO HTTP middleware") {
      for
        events <- Ref.make(Vector.empty[String])
        outer = Middleware.make: (_, next) =>
          events.update(_ :+ "outer-before") *>
            next.tap(_ => events.update(_ :+ "outer-after"))
        inner = Middleware.make: (_, next) =>
          events.update(_ :+ "inner-before") *>
            next.tap(_ => events.update(_ :+ "inner-after"))
        result <- Bedrock.chat("hello").text.provideLayer(
          BedrockMock(BedrockMock.MockBehavior.Reply("world")) @@ (outer ++ inner)
        )
        seen <- events.get
      yield assertTrue(
        result == "world",
        seen == Vector("outer-before", "inner-before", "inner-after", "outer-after"),
      )
    },
    test("can wrap and retry an exchange effect") {
      val failure = Error.Throttling("slow down")
      val middleware = Middleware.make: (_, next) =>
        next.catchSome:
          case _: Error.Throttling => next
      Bedrock.chat("hello").text.provideLayer(
        BedrockMock(
          BedrockMock.MockBehavior.Fail(failure),
          BedrockMock.MockBehavior.Reply("world"),
        ) @@ middleware
      ).map(result => assertTrue(result == "world"))
    },
    test("exposes canonical structured-output request and response bodies") {
      for
        bodies <- Ref.make(Option.empty[(zio.json.ast.Json, zio.json.ast.Json)])
        middleware = Middleware.make: (request, next) =>
          next.tap(response => bodies.set(Some(request.body -> response.body)))
        result <- Bedrock.chat("reply").asResponse[StructuredReply].provideLayer(
          BedrockMock(BedrockMock.MockBehavior.ReplyJson(StructuredReply("ok"))) @@ middleware
        )
        seen <- bodies.get
      yield assertTrue(
        result.output == StructuredReply("ok"),
        seen.flatMap(_._1.asObject).exists(_.get("outputConfig").nonEmpty),
        seen.flatMap(_._2.asObject).exists(_.get("output").nonEmpty),
      )
    },
    test("wraps streaming exchanges") {
      for
        events <- Ref.make(Vector.empty[String])
        middleware = Middleware.make(
          unary = (_, next) => next,
          streaming = (_, next) =>
            ZStream.fromZIO(events.update(_ :+ "stream-before")).drain ++
              next.tap:
                case StreamEvent.Complete(_) => events.update(_ :+ "stream-complete")
                case _                       => ZIO.unit,
        )
        text <- Bedrock.chat("hello").textStream.runCollect.provideLayer(
          BedrockMock(BedrockMock.MockBehavior.Reply("world")) @@ middleware
        )
        seen <- events.get
      yield assertTrue(
        text.mkString == "world",
        seen == Vector("stream-before", "stream-complete"),
      )
    },
  )
