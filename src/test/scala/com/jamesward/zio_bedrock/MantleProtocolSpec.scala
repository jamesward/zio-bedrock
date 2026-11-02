package com.jamesward.zio_bedrock

import com.jamesward.zio_bedrock.Bedrock.*
import zio.*
import zio.http.{Client as HttpClient, Request as HttpRequest, *}
import zio.json.*
import zio.json.ast.Json
import zio.schema.{Schema, derived}
import zio.test.*
import zio.test.TestAspect.*

/** Protocol integration tests against a real local HTTP server. These exercise
  * the complete ZIO HTTP transport rather than mocking `Bedrock`.
  */
object MantleProtocolSpec extends ZIOSpecDefault:

  case class Forecast(city: String, summary: String) derives Schema
  case class WeatherInput(city: String) derives Schema
  case class WeatherOutput(temperatureF: Int, conditions: String) derives Schema

  private val model = ModelId("anthropic.claude-sonnet-4-5")

  private def completion(content: String, finishReason: String = "stop", usage: String =
    """{"prompt_tokens":7,"completion_tokens":3,"total_tokens":10}"""): String =
    val encodedContent = content.toJson
    s"""{"id":"chatcmpl-test","object":"chat.completion","created":1,"model":"${model.unwrap}","choices":[{"index":0,"message":{"role":"assistant","content":$encodedContent},"finish_reason":"$finishReason"}],"usage":$usage}"""

  private def toolCompletion: String =
    """{"id":"chatcmpl-tool","object":"chat.completion","created":1,"model":"anthropic.claude-sonnet-4-5","choices":[{"index":0,"message":{"role":"assistant","tool_calls":[{"id":"call-weather","type":"function","function":{"name":"weather","arguments":"{\"city\":\"Paris\"}"}}]},"finish_reason":"tool_calls"}],"usage":{"prompt_tokens":4,"completion_tokens":2,"total_tokens":6}}"""

  private def json(body: String): Json.Obj =
    body.fromJson[Json.Obj].fold(error => throw new AssertionError(error), value => value)

  private def field(value: Json, name: String): Json =
    value.asObject.flatMap(_.get(name)).getOrElse(throw new AssertionError(s"missing '$name' in $value"))

  private def clientLayer(
    httpClient: HttpClient,
    port: Int,
    api: MantleApi = MantleApi.Standard,
  ): ULayer[Bedrock] =
    val endpoint = URL.decode(s"http://localhost:$port").toOption.get
    ZLayer.succeed(httpClient) >>> Mantle.layer(MantleConfig(
      ApiKey("test-secret"),
      Region.UsEast1,
      model,
      endpoint,
      api,
    ))

  def spec = suite("Bedrock Mantle protocol integration")(
    test("asJsonObject asks for one forced tool call taking the runtime JSON Schema, and gives its arguments") {
      def call(name: String, arguments: String) =
        s"""{"id":"c","object":"chat.completion","created":1,"model":"m","choices":[{"index":0,"message":{"role":"assistant","content":"We need to read it.","tool_calls":[{"id":"t1","type":"function","function":{"name":"$name","arguments":${arguments.toJson}}}]},"finish_reason":"tool_calls"}],"usage":{"prompt_tokens":4,"completion_tokens":2,"total_tokens":6}}"""
      for
        captured <- Promise.make[Nothing, String]
        routes = Routes(
          Method.POST / "v1" / "chat" / "completions" -> handler { (request: HttpRequest) =>
            request.body.asString.orDie.flatMap: body =>
              captured.succeed(body) *> ZIO.succeed(Response.json(call("arguments", """{"customerId":"C0046"}""")))
          },
        )
        port   <- Server.install(routes)
        http   <- ZIO.service[HttpClient]
        schema  = Json.Obj(
                    "type" -> Json.Str("object"),
                    "properties" -> Json.Obj("customerId" -> Json.Obj("type" -> Json.Str("string"))),
                    "required" -> Json.Arr(Json.Str("customerId")),
                  )
        result <- Bedrock.chat("Arguments for list_orders").asJsonObject(schema, "arguments", Some("Arguments for list_orders"))
                    .provideLayer(clientLayer(http, port))
        body   <- captured.await.map(json)
        tools   = field(body, "tools").asArray.toList.flatten
        function = field(tools.head, "function")
      yield assertTrue(
        result.output.toJson == """{"customerId":"C0046"}""",
        result.usage.outputTokens == 2,
        tools.size == 1,
        field(function, "name").asString.contains("arguments"),
        field(function, "description").asString.contains("Arguments for list_orders"),
        field(field(function, "parameters"), "required").toJson == """["customerId"]""",
        field(body, "tool_choice").toJson == """{"type":"function","function":{"name":"arguments"}}""",
        body.get("response_format").isEmpty,
      )
    },
    test("a middleware over Mantle keeps asJsonObject's forced tool call") {
      for
        captured <- Promise.make[Nothing, String]
        routes = Routes(
          Method.POST / "v1" / "chat" / "completions" -> handler { (request: HttpRequest) =>
            request.body.asString.orDie.flatMap(body => captured.succeed(body) *> ZIO.succeed(Response.json(completion("{}"))))
          },
        )
        port   <- Server.install(routes)
        http   <- ZIO.service[HttpClient]
        logged  = BedrockMiddleware.observe((_, _, _) => ZIO.unit)
        _      <- Bedrock.chat("x").asJsonObject(Json.Obj("type" -> Json.Str("object")), "arguments")
                    .provideLayer(clientLayer(http, port) @@ logged).either
        body   <- captured.await.map(json)
      yield assertTrue(body.get("tool_choice").isDefined, body.get("response_format").isEmpty)
    },
    test("asJsonObject fails when the model doesn't call the forced tool; another tool's call, or text, counts when it fits") {
      val replies = List(completion("""{"customerId":"C0046"}"""), toolCompletion, toolCompletion, completion("Done: {\"city\":\"Rome\"}"))
      for
        index  <- Ref.make(0)
        routes  = Routes(
                    Method.POST / "v1" / "chat" / "completions" -> handler { (_: HttpRequest) =>
                      index.getAndUpdate(_ + 1).map(i => Response.json(replies(i)))
                    },
                  )
        port   <- Server.install(routes)
        http   <- ZIO.service[HttpClient]
        schema  = Json.Obj("type" -> Json.Str("object"), "properties" -> Json.Obj("city" -> Json.Obj("type" -> Json.Str("string"))))
        other   = Json.Obj("type" -> Json.Str("object"), "required" -> Json.Arr(Json.Str("file_path")))
        got    <- ZIO.foreach(List(schema, schema, other, schema))(s => Bedrock.chat("x").asJsonObject(s, "arguments").provideLayer(clientLayer(http, port)).either)
      yield assertTrue(
        got(0).left.toOption.map(_.toString).contains(Error.MissingToolUse(ToolName("arguments")).toString),
        got(1).map(_.output.toJson) == Right("""{"city":"Paris"}"""),
        got(3).map(_.output.toJson) == Right("""{"city":"Rome"}"""),
        got(2).left.toOption.map(_.toString).contains(Error.OtherToolUse(ToolName("arguments"), ToolName("weather"), Json.Obj("city" -> Json.Str("Paris"))).toString),
      )
    },
    test("encodes OpenAI chat, tools, structured output, inference config, and bearer auth") {
      for
        captured <- Promise.make[Nothing, (HttpRequest, String)]
        routes = Routes(
          Method.POST / "v1" / "chat" / "completions" -> handler { (request: HttpRequest) =>
            request.body.asString.orDie.flatMap: body =>
              captured.succeed(request -> body) *>
                ZIO.succeed(Response.json(completion("Expect{\"city\":\"Seattle\",\"summary\":\"Rain\"}")))
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
        pair <- captured.await
        (request, body) = pair
        obj = json(body)
        messages = field(obj, "messages").asArray.getOrElse(Chunk.empty)
        roles = messages.flatMap(_.asObject.flatMap(_.get("role")).flatMap(_.asString))
        function = field(field(field(obj, "tools").asArray.get.head, "function"), "parameters")
      yield assertTrue(
        request.rawHeader("authorization").contains("Bearer test-secret"),
        field(obj, "model").asString.contains(model.unwrap),
        roles == Chunk("system", "user"),
        field(obj, "max_tokens").asNumber.exists(_.value.intValue == 200),
        field(obj, "temperature").asNumber.exists(_.value.doubleValue == 0.2),
        field(obj, "top_p").asNumber.exists(_.value.doubleValue == 0.9),
        field(obj, "stop").asArray.exists(_.flatMap(_.asString) == Chunk("END")),
        field(obj, "tool_choice").asString.contains("auto"),
        field(function, "type").asString.contains("object"),
        field(obj, "response_format").asObject
          .flatMap(_.get("type")).flatMap(_.asString).contains("json_schema"),
        result.output.city == "Seattle",
        result.output.summary == "Rain",
        result.usage.inputTokens == 7,
        result.usage.outputTokens == 3,
        result.usage.totalTokens == 10,
        result.metrics.latencyMs == 0L,
        result.stopReason == StopReason.EndTurn,
      )
    },

    test("uses the OpenAI API base when configured") {
      for
        captured <- Promise.make[Nothing, Unit]
        routes = Routes(
          Method.POST / "openai" / "v1" / "chat" / "completions" -> handler { (_: HttpRequest) =>
            captured.succeed(()) *>
              ZIO.succeed(Response.json(completion("ok")))
          },
        )
        port <- Server.install(routes)
        http <- ZIO.service[HttpClient]
        result <- Bedrock.chat("hello").text.provideLayer(clientLayer(http, port, MantleApi.OpenAI))
        _ <- captured.await
      yield assertTrue(result == "ok")
    },
    test("retains a custom endpoint path before the standard API base") {
      for
        captured <- Promise.make[Nothing, Unit]
        routes = Routes(
          Method.POST / "openai" / "v1" / "chat" / "completions" -> handler { (_: HttpRequest) =>
            captured.succeed(()) *>
              ZIO.succeed(Response.json(completion("ok")))
          },
        )
        port <- Server.install(routes)
        http <- ZIO.service[HttpClient]
        endpoint = URL.decode(s"http://localhost:$port/openai").toOption.get
        layer = ZLayer.succeed(http) >>> Mantle.layer(MantleConfig(ApiKey("test-secret"), endpoint, model))
        result <- Bedrock.chat("hello").text.provideLayer(layer)
        _ <- captured.await
      yield assertTrue(result == "ok")
    },
    test("loop sends assistant tool_calls followed by role=tool result") {
      for
        bodies <- Ref.make(List.empty[String])
        calls <- Ref.make(0)
        routes = Routes(
          Method.POST / "v1" / "chat" / "completions" -> handler { (request: HttpRequest) =>
            for
              body <- request.body.asString.orDie
              _ <- bodies.update(_ :+ body)
              call <- calls.getAndUpdate(_ + 1)
            yield Response.json(if call == 0 then toolCompletion else completion("Paris is 64F and foggy."))
          },
        )
        port <- Server.install(routes)
        http <- ZIO.service[HttpClient]
        tools = (
          weather = ToolHandler.fromPure(
            (in: WeatherInput) => WeatherOutput(64, s"foggy in ${in.city}"),
            "Get weather",
          ),
        )
        answer <- Bedrock.loop("Weather in Paris?", tools).text.provideLayer(clientLayer(http, port))
        requests <- bodies.get
        second = json(requests(1))
        messages = field(second, "messages").asArray.getOrElse(Chunk.empty)
        roles = messages.flatMap(_.asObject.flatMap(_.get("role")).flatMap(_.asString))
        assistant = messages(1).asObject.get
        toolResult = messages(2).asObject.get
      yield assertTrue(
        answer.contains("64F"),
        requests.size == 2,
        roles == Chunk("user", "assistant", "tool"),
        assistant.get("tool_calls").flatMap(_.asArray).exists(_.nonEmpty),
        toolResult.get("tool_call_id").flatMap(_.asString).contains("call-weather"),
        toolResult.get("content").flatMap(_.asString).exists(_.contains("64")),
      )
    },

    test("decodes standard Chat Completions SSE on the same endpoint") {
      val sse =
        """data: {"id":"chatcmpl-stream","object":"chat.completion.chunk","created":1,"model":"anthropic.claude-sonnet-4-5","choices":[{"index":0,"delta":{"role":"assistant"}}]}
          |
          |data: {"id":"chatcmpl-stream","object":"chat.completion.chunk","created":1,"model":"anthropic.claude-sonnet-4-5","choices":[{"index":0,"delta":{"content":"hel"}}]}
          |
          |data: {"id":"chatcmpl-stream","object":"chat.completion.chunk","created":1,"model":"anthropic.claude-sonnet-4-5","choices":[{"index":0,"delta":{"content":"lo"}}]}
          |
          |data: {"id":"chatcmpl-stream","object":"chat.completion.chunk","created":1,"model":"anthropic.claude-sonnet-4-5","choices":[{"index":0,"delta":{},"finish_reason":"stop"}]}
          |
          |data: {"id":"chatcmpl-stream","object":"chat.completion.chunk","created":1,"model":"anthropic.claude-sonnet-4-5","choices":[],"usage":{"prompt_tokens":2,"completion_tokens":1,"total_tokens":3}}
          |
          |data: [DONE]
          |
          |""".stripMargin
      for
        captured <- Promise.make[Nothing, String]
        routes = Routes(
          Method.POST / "v1" / "chat" / "completions" -> handler { (request: HttpRequest) =>
            request.body.asString.orDie.flatMap: body =>
              captured.succeed(body) *>
                ZIO.succeed(Response.text(sse).addHeader("content-type", "text/event-stream"))
          },
        )
        port <- Server.install(routes)
        http <- ZIO.service[HttpClient]
        chunks <- Bedrock.chat("Say hello").textStream.runCollect.provideLayer(clientLayer(http, port))
        body <- captured.await
        request = json(body)
        events <- Bedrock.chat("Say hello").asStream.runCollect.provideLayer(clientLayer(http, port))
        complete = events.collectFirst { case StreamEvent.Complete(value) => value.result }
      yield assertTrue(
        chunks.mkString == "hello",
        field(request, "stream").asBoolean.contains(true),
        field(field(request, "stream_options"), "include_usage").asBoolean.contains(true),
        complete.exists(_.output.text == "hello"),
        complete.exists(value => value.usage.inputTokens == 2 && value.usage.outputTokens == 1 && value.usage.totalTokens == 3),
        complete.exists(_.metrics.latencyMs == 0L),
      )
    },

    test("rejects an SSE response that ends before finish_reason") {
      val truncated =
        """data: {"id":"chatcmpl-stream","object":"chat.completion.chunk","created":1,"model":"anthropic.claude-sonnet-4-5","choices":[{"index":0,"delta":{"content":"partial"}}]}
          |
          |""".stripMargin
      for
        routes = Routes(
          Method.POST / "v1" / "chat" / "completions" -> handler(
            Response.text(truncated).addHeader("content-type", "text/event-stream"),
          ),
        )
        port <- Server.install(routes)
        http <- ZIO.service[HttpClient]
        exit <- Bedrock.chat("Say hello").asStream.runCollect.provideLayer(clientLayer(http, port)).exit
      yield assertTrue(
        exit.causeOption.flatMap(_.failureOption).exists {
          case Error.Unexpected(_, body) => body.contains("before finish_reason")
          case _                         => false
        },
      )
    },
  ).provide(Server.defaultWith(_.onAnyOpenPort), HttpClient.default) @@ sequential @@ withLiveClock @@ timeout(30.seconds)
