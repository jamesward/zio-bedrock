package com.jamesward.zio_bedrock

import com.jamesward.zio_bedrock.Bedrock.*
import zio.*
import zio.http.{Client as HttpClient, Request as HttpRequest, *}
import zio.json.*
import zio.json.ast.Json
import zio.schema.{Schema, derived}
import zio.test.*
import zio.test.TestAspect.*

import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets

/** Full native Converse transport tests against a real local ZIO HTTP server. */
object ConverseProtocolSpec extends ZIOSpecDefault:

  case class Forecast(city: String, summary: String) derives Schema
  case class WeatherInput(city: String) derives Schema
  case class WeatherOutput(temperatureF: Int, conditions: String) derives Schema

  private val model = ModelId("provider:model id")

  private val responseJson =
    """{"output":{"message":{"role":"assistant","content":[{"reasoningContent":{"reasoningText":{"text":"thinking","signature":"sig-1"}}},{"text":"prefix {\"city\":\"Seattle\",\"summary\":\"Rain\"}"}]}},"stopReason":"end_turn","usage":{"inputTokens":7,"outputTokens":3,"totalTokens":10,"cacheReadInputTokens":2,"cacheWriteInputTokens":1},"metrics":{"latencyMs":42}}"""

  private val toolResponseJson =
    """{"output":{"message":{"role":"assistant","content":[{"toolUse":{"toolUseId":"call-weather","name":"weather","input":{"city":"Paris"}}}]}},"stopReason":"tool_use","usage":{"inputTokens":4,"outputTokens":2,"totalTokens":6},"metrics":{"latencyMs":8}}"""

  private val finalResponseJson =
    """{"output":{"message":{"role":"assistant","content":[{"text":"Paris is 64F and foggy."}]}},"stopReason":"end_turn","usage":{"inputTokens":9,"outputTokens":4,"totalTokens":13},"metrics":{"latencyMs":11}}"""

  private def json(body: String): Json.Obj =
    body.fromJson[Json.Obj].fold(error => throw new AssertionError(error), value => value)

  private def field(value: Json, name: String): Json =
    value.asObject.flatMap(_.get(name)).getOrElse(throw new AssertionError(s"missing '$name' in $value"))

  private def clientLayer(httpClient: HttpClient, port: Int): ULayer[Bedrock] =
    val endpoint = URL.decode(s"http://localhost:$port").toOption.get
    ZLayer.succeed(httpClient) >>> Converse.layer(ConverseConfig(ApiKey("test-secret"), endpoint, model))

  private def stringHeader(name: String, value: String): Array[Byte] =
    val nameBytes = name.getBytes(StandardCharsets.UTF_8)
    val valueBytes = value.getBytes(StandardCharsets.UTF_8)
    ByteBuffer.allocate(1 + nameBytes.length + 1 + 2 + valueBytes.length)
      .put(nameBytes.length.toByte)
      .put(nameBytes)
      .put(7.toByte)
      .putShort(valueBytes.length.toShort)
      .put(valueBytes)
      .array()

  private def frame(eventType: String, payload: String, messageType: String = "event"): Chunk[Byte] =
    val headers = stringHeader(":message-type", messageType) ++
      stringHeader(if messageType == "exception" then ":exception-type" else ":event-type", eventType) ++
      stringHeader(":content-type", "application/json")
    val payloadBytes = payload.getBytes(StandardCharsets.UTF_8)
    val totalLength = 16 + headers.length + payloadBytes.length
    val prelude = ByteBuffer.allocate(8).putInt(totalLength).putInt(headers.length).array()
    val preludeChecksum = crc32(prelude)
    val withoutMessageChecksum = ByteBuffer.allocate(totalLength - 4)
      .put(prelude)
      .putInt(preludeChecksum.toInt)
      .put(headers)
      .put(payloadBytes)
      .array()
    Chunk.fromArray(
      ByteBuffer.allocate(totalLength)
        .put(withoutMessageChecksum)
        .putInt(crc32(withoutMessageChecksum).toInt)
        .array(),
    )

  private def crc32(bytes: Array[Byte]): Long =
    val checksum = new java.util.zip.CRC32
    checksum.update(bytes)
    checksum.getValue

  def spec = suite("Bedrock Converse protocol integration")(
    test("encodes native JSON and decodes structured output, reasoning, usage, cache, latency, auth, and model path") {
      for
        captured <- Promise.make[Nothing, (HttpRequest, String)]
        routes = Routes(
          RoutePattern.any -> handler { (request: HttpRequest) =>
            request.body.asString.orDie.flatMap: body =>
              captured.succeed(request -> body) *> ZIO.succeed(Response.json(responseJson))
          },
        )
        port <- Server.install(routes)
        http <- ZIO.service[HttpClient]
        tool = Tool[WeatherInput](ToolName("weather"), "Get weather")
        config = RequestConfig(
          messages = List(Message.user("Forecast Seattle")),
          system = "Be concise",
          inferenceConfig = InferenceConfig(maxTokens = 200, temperature = 0.2, topP = 0.9, stopSequences = List("END")),
          toolConfig = ToolConfig(List(tool), ToolChoice.Auto),
        )
        result <- Bedrock.chat(config).asResponse[Forecast].provideLayer(clientLayer(http, port))
        (request, body) <- captured.await
        obj = json(body)
        message = field(obj, "messages").asArray.get.head
        system = field(obj, "system").asArray.get.head
        toolSpec = field(field(obj, "toolConfig"), "tools").asArray.get.head
      yield assertTrue(
        request.rawHeader("authorization").contains("Bearer test-secret"),
        request.url.path.encode == "/model/provider:model%20id/converse",
        field(message, "role").asString.contains("user"),
        field(field(message, "content").asArray.get.head, "text").asString.contains("Forecast Seattle"),
        field(system, "text").asString.contains("Be concise"),
        field(field(obj, "inferenceConfig"), "maxTokens").asNumber.exists(_.value.intValue == 200),
        field(field(obj, "inferenceConfig"), "stopSequences").asArray.exists(_.flatMap(_.asString) == Chunk("END")),
        field(field(field(toolSpec, "toolSpec"), "inputSchema"), "json").asObject.flatMap(_.get("type")).flatMap(_.asString).contains("object"),
        field(field(obj, "toolConfig"), "toolChoice").asObject.flatMap(_.get("auto")).flatMap(_.asObject).isDefined,
        field(field(obj, "outputConfig"), "textFormat").asObject.flatMap(_.get("type")).flatMap(_.asString).contains("json_schema"),
        result.output.city == "Seattle",
        result.output.summary == "Rain",
        result.stopReason == StopReason.EndTurn,
        result.usage.inputTokens == 7,
        result.usage.outputTokens == 3,
        result.usage.totalTokens == 10,
        result.usage.cacheReadInputTokens.asInstanceOf[Int] == 2,
        result.usage.cacheWriteInputTokens.asInstanceOf[Int] == 1,
        result.metrics.latencyMs.asInstanceOf[Long] == 42L,
      )
    },

    test("loop sends assistant tool use and native user tool result history") {
      for
        bodies <- Ref.make(List.empty[String])
        calls <- Ref.make(0)
        routes = Routes(
          RoutePattern.any -> handler { (request: HttpRequest) =>
            for
              body <- request.body.asString.orDie
              _ <- bodies.update(_ :+ body)
              call <- calls.getAndUpdate(_ + 1)
            yield Response.json(if call == 0 then toolResponseJson else finalResponseJson)
          },
        )
        port <- Server.install(routes)
        http <- ZIO.service[HttpClient]
        tools = (
          weather = ToolHandler.fromPure(
            (input: WeatherInput) => WeatherOutput(64, s"foggy in ${input.city}"),
            "Get weather",
          ),
        )
        answer <- Bedrock.loop("Weather in Paris?", tools).text.provideLayer(clientLayer(http, port))
        requests <- bodies.get
        second = json(requests(1))
        messages = field(second, "messages").asArray.get
        assistant = messages(1)
        toolResult = messages(2)
      yield assertTrue(
        answer.contains("64F"),
        requests.size == 2,
        field(assistant, "role").asString.contains("assistant"),
        field(field(assistant, "content").asArray.get.head, "toolUse").asObject
          .flatMap(_.get("toolUseId")).flatMap(_.asString).contains("call-weather"),
        field(toolResult, "role").asString.contains("user"),
        field(field(toolResult, "content").asArray.get.head, "toolResult").asObject
          .flatMap(_.get("toolUseId")).flatMap(_.asString).contains("call-weather"),
        field(field(field(toolResult, "content").asArray.get.head, "toolResult"), "content").asArray
          .exists(_.head.asObject.flatMap(_.get("json")).flatMap(_.asObject).flatMap(_.get("temperatureF")).isDefined),
      )
    },

    test("decodes AWS EventStream text, reasoning, tools, stop reason, cache usage, and latency") {
      val bytes =
        frame("contentBlockStart", """{"contentBlockIndex":0,"start":{"toolUse":{"toolUseId":"call-1","name":"weather"}}}""") ++
        frame("contentBlockDelta", """{"contentBlockIndex":0,"delta":{"toolUse":{"input":"{\"city\":"}}}""") ++
        frame("contentBlockDelta", """{"contentBlockIndex":0,"delta":{"toolUse":{"input":"\"Paris\"}"}}}""") ++
        frame("contentBlockStop", """{"contentBlockIndex":0}""") ++
        frame("contentBlockDelta", """{"contentBlockIndex":1,"delta":{"reasoningContent":{"text":"think"}}}""") ++
        frame("contentBlockDelta", """{"contentBlockIndex":2,"delta":{"text":"hello\n"}}""") ++
        frame("messageStop", """{"stopReason":"stop_sequence"}""") ++
        frame("metadata", """{"usage":{"inputTokens":5,"outputTokens":2,"totalTokens":7,"cacheReadInputTokens":3,"cacheWriteInputTokens":1},"metrics":{"latencyMs":1234}}""")
      for
        captured <- Promise.make[Nothing, (String, String)]
        routes = Routes(
          RoutePattern.any -> handler { (request: HttpRequest) =>
            request.body.asString.orDie.flatMap: body =>
              captured.succeed(request.url.path.encode -> body) *>
                ZIO.succeed(Response(body = Body.fromChunk(bytes)))
          },
        )
        port <- Server.install(routes)
        http <- ZIO.service[HttpClient]
        events <- Bedrock.chat("stream").asStream.runCollect.provideLayer(clientLayer(http, port))
        (path, requestBody) <- captured.await
        usage = events.collectFirst { case StreamEvent.Metadata(value, _) => value }
        metrics = events.collectFirst { case StreamEvent.Metadata(_, value) => value }
        complete = events.collectFirst { case StreamEvent.Complete(value) => value.result }
      yield assertTrue(
        path == "/model/provider:model%20id/converse-stream",
        json(requestBody).get("messages").isDefined,
        events.contains(StreamEvent.ToolUseStart(ToolUseId("call-1"), ToolName("weather"))),
        events.collect { case StreamEvent.ToolUseDelta(_, value) => value }.mkString == "{\"city\":\"Paris\"}",
        events.contains(StreamEvent.ContentBlockStop(0)),
        events.contains(StreamEvent.ReasoningDelta("think")),
        events.contains(StreamEvent.TextDelta("hello\n")),
        events.contains(StreamEvent.MessageStop(StopReason.StopSequence)),
        usage.contains(TokenUsage(5, 2, 7, 3, 1)),
        metrics.exists(_.latencyMs.asInstanceOf[Long] == 1234L),
        complete.exists(_.stopReason == StopReason.StopSequence),
        complete.exists(_.output.message.content.collect { case ContentBlock.Text(text) => text }.mkString == "hello\n"),
        complete.exists(_.output.message.content.exists {
          case ContentBlock.ToolUse(id, name, input) =>
            id == ToolUseId("call-1") && name == ToolName("weather") && input.as[WeatherInput].contains(WeatherInput("Paris"))
          case _ => false
        }),
      )
    },

    test("streaming loop continues with the exact signed assistant turn") {
      val first =
        frame("contentBlockDelta", """{"contentBlockIndex":0,"delta":{"reasoningContent":{"text":"thinking","signature":"signed-1"}}}""") ++
        frame("contentBlockStop", """{"contentBlockIndex":0}""") ++
        frame("contentBlockStart", """{"contentBlockIndex":1,"start":{"toolUse":{"toolUseId":"call-weather","name":"weather"}}}""") ++
        frame("contentBlockDelta", """{"contentBlockIndex":1,"delta":{"toolUse":{"input":"{\"city\":\"Paris\"}"}}}""") ++
        frame("contentBlockStop", """{"contentBlockIndex":1}""") ++
        frame("messageStop", """{"stopReason":"tool_use"}""") ++
        frame("metadata", """{"usage":{"inputTokens":3,"outputTokens":2,"totalTokens":5},"metrics":{"latencyMs":9}}""")
      val second =
        frame("contentBlockDelta", """{"contentBlockIndex":0,"delta":{"text":"done"}}""") ++
        frame("contentBlockStop", """{"contentBlockIndex":0}""") ++
        frame("messageStop", """{"stopReason":"end_turn"}""") ++
        frame("metadata", """{"usage":{"inputTokens":4,"outputTokens":1,"totalTokens":5},"metrics":{"latencyMs":7}}""")
      for
        bodies <- Ref.make(List.empty[String])
        calls <- Ref.make(0)
        routes = Routes(RoutePattern.any -> handler { (request: HttpRequest) =>
          for
            body <- request.body.asString.orDie
            _ <- bodies.update(_ :+ body)
            call <- calls.getAndUpdate(_ + 1)
          yield Response(body = Body.fromChunk(if call == 0 then first else second))
        })
        port <- Server.install(routes)
        http <- ZIO.service[HttpClient]
        tools = (weather = ToolHandler.fromPure(
          (input: WeatherInput) => WeatherOutput(64, s"foggy in ${input.city}"),
          "Get weather",
        ))
        text <- Bedrock.loop("Weather?", tools).textStream.runCollect.provideLayer(clientLayer(http, port))
        requests <- bodies.get
        continued = json(requests(1))
        messages = field(continued, "messages").asArray.get
        assistantContent = field(messages(1), "content").asArray.get
      yield assertTrue(
        text.mkString == "done",
        requests.size == 2,
        assistantContent.head.asObject.flatMap(_.get("reasoningContent")).flatMap(_.asObject)
          .flatMap(_.get("reasoningText")).flatMap(_.asObject).flatMap(_.get("signature")).flatMap(_.asString).contains("signed-1"),
        assistantContent.exists(_.asObject.flatMap(_.get("toolUse")).flatMap(_.asObject)
          .flatMap(_.get("toolUseId")).flatMap(_.asString).contains("call-weather")),
        field(messages(2), "content").asArray.exists(_.head.asObject.flatMap(_.get("toolResult")).flatMap(_.asObject)
          .flatMap(_.get("toolUseId")).flatMap(_.asString).contains("call-weather")),
      )
    },

    test("low-level follow-up retains signed reasoning until the message is made portable") {
      for
        bodies <- Ref.make(List.empty[String])
        routes = Routes(RoutePattern.any -> handler { (request: HttpRequest) =>
          request.body.asString.orDie.flatMap: body =>
            bodies.update(_ :+ body).as(Response.json(responseJson))
        })
        port <- Server.install(routes)
        http <- ZIO.service[HttpClient]
        backend = clientLayer(http, port)
        first <- Bedrock.chat("first").asResponse.provideLayer(backend)
        _ <- Bedrock.chat(RequestConfig(List(
          Message.user("first"),
          first.output.message,
          Message.user("continue"),
        ))).text.provideLayer(backend)
        _ <- Bedrock.chat(RequestConfig(List(
          Message.user("first"),
          first.output.message.portable,
          Message.user("continue portably"),
        ))).text.provideLayer(backend)
        requests <- bodies.get
        retainedContent = field(field(json(requests(1)), "messages").asArray.get(1), "content").asArray.get
        portableContent = field(field(json(requests(2)), "messages").asArray.get(1), "content").asArray.get
      yield assertTrue(
        retainedContent.exists(_.asObject.flatMap(_.get("reasoningContent")).flatMap(_.asObject)
          .flatMap(_.get("reasoningText")).flatMap(_.asObject)
          .flatMap(_.get("signature")).flatMap(_.asString).contains("sig-1")),
        portableContent.forall(_.asObject.flatMap(_.get("reasoningContent")).isEmpty),
        portableContent.exists(_.asObject.flatMap(_.get("text")).flatMap(_.asString).exists(_.startsWith("prefix"))),
      )
    },

    test("maps HTTP and EventStream failures to shared Bedrock errors") {
      val exception = frame("throttlingException", """{"message":"slow down"}""", "exception")
      for
        routes = Routes(
          RoutePattern.any -> handler { (request: HttpRequest) =>
            if request.url.path.encode.endsWith("/converse-stream") then
              ZIO.succeed(Response(body = Body.fromChunk(exception)))
            else
              ZIO.succeed(Response(status = Status.Forbidden, body = Body.fromString("denied")))
          },
        )
        port <- Server.install(routes)
        http <- ZIO.service[HttpClient]
        regular <- Bedrock.chat("no").text.provideLayer(clientLayer(http, port)).exit
        streamed <- Bedrock.chat("no").textStream.runCollect.provideLayer(clientLayer(http, port)).exit
      yield assertTrue(
        regular.causeOption.flatMap(_.failureOption).exists(_.isInstanceOf[Error.AccessDenied]),
        streamed.causeOption.flatMap(_.failureOption).contains(Error.Throttling("slow down")),
      )
    },

    test("rejects an AWS EventStream frame with an invalid checksum") {
      val corrupted = frame("metadata", """{"usage":{"inputTokens":1,"outputTokens":1,"totalTokens":2},"metrics":{"latencyMs":1}}""").toArray
      corrupted(12) = (corrupted(12) ^ 1).toByte
      for
        routes = Routes(RoutePattern.any -> handler(Response(body = Body.fromChunk(Chunk.fromArray(corrupted)))))
        port <- Server.install(routes)
        http <- ZIO.service[HttpClient]
        exit <- Bedrock.chat("no").asStream.runCollect.provideLayer(clientLayer(http, port)).exit
      yield assertTrue(
        exit.causeOption.flatMap(_.failureOption).exists {
          case Error.Unexpected(_, body) => body.contains("checksum")
          case _                         => false
        },
      )
    },
  ).provide(Server.defaultWith(_.onAnyOpenPort), HttpClient.default) @@ sequential @@ withLiveClock @@ timeout(30.seconds)
