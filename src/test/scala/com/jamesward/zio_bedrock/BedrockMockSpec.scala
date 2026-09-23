package com.jamesward.zio_bedrock

import com.jamesward.zio_bedrock.Bedrock.*
import zio.*
import zio.json.ast.Json
import zio.test.*

/**
 * Single-turn `Bedrock.chat` scenarios run against the deterministic
 * mock. No tools, no loop — just text and structured output (plus the
 * normalized tool-call response shape via the mock-only list).
 */
object BedrockMockSpec extends ZIOSpecDefault:

  private def asTest(s: SharedSpec.BedrockScenario): Spec[Any, Any] =

    test(s.name):
      s.run.provideLayer(BedrockMock(s.mockScript*))

  def spec =
    val forcedToolTerminals = suite("forced tool terminals")(
      test("returns one expected JSON object with response metadata") {
        val name = ToolName("weather")
        Bedrock.chat(RequestConfig(
          messages = List(Message.user("weather")),
          toolConfig = ToolConfig(List(Tool[SharedSpec.WeatherInput](name, "weather")), ToolChoice.Tool(name)),
        )).asForcedToolObject(name).provideLayer(
          BedrockMock(BedrockMock.MockBehavior.CallTool(name, SharedSpec.WeatherInput("Denver"))),
        ).map(result => assertTrue(
          result.output.get("city").flatMap(_.asString).contains("Denver"),
          result.metrics.latencyMs == 0L,
        ))
      },
      test("fails with typed missing, multiple, unexpected, and invalid-input errors") {
        val expected = ToolName("expected")
        for
          missing <- Bedrock.chat("hello").asForcedToolUse(expected)
            .provideLayer(BedrockMock(BedrockMock.MockBehavior.Reply("no tool"))).either
          multiple <- Bedrock.chat("tools").asForcedToolUse(expected)
            .provideLayer(BedrockMock(BedrockMock.MockBehavior.CallTools(List(
              expected -> Json.Obj("value" -> Json.Str("one")),
              expected -> Json.Obj("value" -> Json.Str("two")),
            )))).either
          unexpected <- Bedrock.chat(RequestConfig(
            messages = List(Message.user("tool")),
            toolConfig = ToolConfig(List(Tool[SharedSpec.WeatherInput](ToolName("other"), "other"))),
          )).asForcedToolUse(expected)
            .provideLayer(BedrockMock(BedrockMock.MockBehavior.CallTool(ToolName("other"), SharedSpec.WeatherInput("Denver")))).either
          invalid <- Bedrock.chat("tool").asForcedToolObject(expected)
            .provideLayer(BedrockMock(BedrockMock.MockBehavior.CallTools(List(expected -> Json.Str("not-an-object"))))).either
        yield assertTrue(
          missing.left.toOption.exists(_.isInstanceOf[Error.MissingToolUse]),
          multiple.left.toOption.exists {
            case Error.MultipleToolUses(name, actual) => name == expected && actual == 2
            case _                                    => false
          },
          unexpected.left.toOption.exists(_.isInstanceOf[Error.UnexpectedToolUse]),
          invalid.left.toOption.exists(_.isInstanceOf[Error.InvalidToolInput]),
        )
      },
    )
    val intermediateTextRegression = test("loop textStream suppresses text from intermediate tool turns") {
      val tools = (
        weather = ToolHandler.fromPure(SharedSpec.get_weather, "Get weather"),
      )
      Bedrock.loop("Weather?", tools).textStream.runCollect.provideLayer(
        BedrockMock(
          BedrockMock.MockBehavior.CallToolWithText(
            "intermediate commentary that must not escape",
            ToolName("weather"),
            SharedSpec.WeatherInput("Denver"),
          ),
          BedrockMock.MockBehavior.Reply("final answer"),
        ),
      ).map(chunks => assertTrue(chunks.mkString == "final answer"))
    }
    val shared = (SharedSpec.bedrockScenarios
      ++ SharedSpec.bedrockRequestScenarios
      ++ SharedSpec.bedrockRequestMockOnlyScenarios
      ++ SharedSpec.bedrockLoopScenarios
      ++ SharedSpec.bedrockLoopIntegrationScenarios
      ++ List(SharedSpec.textStreamScenario, SharedSpec.loopTextStreamScenario, SharedSpec.loopAsStreamScenario)).map(asTest)
    suite("Bedrock Mock")((forcedToolTerminals :: intermediateTextRegression :: shared)*)
