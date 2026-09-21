package com.jamesward.zio_bedrock

import com.jamesward.zio_bedrock.Bedrock.*
import zio.*
import zio.http.Client
import zio.test.*
import zio.test.TestAspect.*

/** Public-API smoke tests against native Converse, enabled when
  * `AWS_BEARER_TOKEN_BEDROCK` is available.
  */
object ConverseIntegrationSpec extends ZIOSpecDefault:
  private def envOr(name: String, default: String): String =
    Option(java.lang.System.getenv(name)).getOrElse(default)

  private val model = ModelId(envOr(
    "BEDROCK_CONVERSE_TEST_MODEL_ID",
    "us.anthropic.claude-sonnet-4-5-20250929-v1:0",
  ))
  private val region = Region.fromCode(envOr("BEDROCK_CONVERSE_TEST_REGION", Region.UsEast1.code))
    .getOrElse(Region.UsEast1)
  private val apiKey = ApiKey(envOr("AWS_BEARER_TOKEN_BEDROCK", ""))
  private val layer: ZLayer[Client, Nothing, Bedrock] =
    Converse.layer(ConverseConfig(apiKey, region, model))

  private def asTest(scenario: SharedSpec.BedrockScenario): Spec[Bedrock, Any] =
    test(scenario.name):
      (ZIO.sleep(500.millis) *> scenario.run).retry(
        Schedule.recurWhile[Any]:
          case _: (Error.Throttling | Error.InternalServer | Error.ServiceUnavailable | Error.ModelTimeout) => true
          case _ => false
        && Schedule.exponential(1.second)
        && Schedule.recurs(5),
      )

  def spec = suite("Bedrock Converse Live Integration")(
    (SharedSpec.bedrockLiveScenarios
      ++ SharedSpec.bedrockRequestLiveScenarios
      ++ SharedSpec.bedrockLoopIntegrationScenarios
      ++ List(SharedSpec.textStreamScenario, SharedSpec.loopTextStreamScenario, SharedSpec.loopAsStreamScenario)).map(asTest)*
  ).provideSomeShared[Scope](Client.default, layer)
    @@ ifEnvSet("AWS_BEARER_TOKEN_BEDROCK")
    @@ withLiveClock
    @@ withLiveSystem
    @@ timeout(90.seconds)
    @@ sequential
