package com.jamesward.zio_bedrock

import com.jamesward.zio_bedrock.Bedrock.*
import zio.*
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
    suite("Bedrock Mock")((intermediateTextRegression :: shared)*)
