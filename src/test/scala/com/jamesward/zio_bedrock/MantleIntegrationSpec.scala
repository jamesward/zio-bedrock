package com.jamesward.zio_bedrock

import com.jamesward.zio_bedrock.Bedrock.*
import zio.*
import zio.http.Client
import zio.test.*
import zio.test.TestAspect.*

/**
 * Public API scenarios run against the live Bedrock Mantle service.
 * Gated by `AWS_BEARER_TOKEN_BEDROCK`; skipped without it.
 *
 * Env overrides:
 *  - `AWS_BEARER_TOKEN_BEDROCK`       — required to enable this suite.
 *  - `BEDROCK_MANTLE_TEST_MODEL_ID` — defaults to `openai.gpt-oss-120b`.
 *  - `BEDROCK_MANTLE_TEST_REGION`   — defaults to `us-east-1`.
 */
object MantleIntegrationSpec extends ZIOSpecDefault:

  private val defaultModelId = "openai.gpt-oss-120b"

  private def envOr(name: String, default: String): String =
    Option(java.lang.System.getenv(name)).getOrElse(default)

  private val testModelId: ModelId = ModelId(envOr("BEDROCK_MANTLE_TEST_MODEL_ID", defaultModelId))
  private val testRegion:  Region  =
    Region.fromCode(envOr("BEDROCK_MANTLE_TEST_REGION", Region.UsEast1.code)).getOrElse(Region.UsEast1)
  private val testApiKey:  ApiKey  = ApiKey(envOr("AWS_BEARER_TOKEN_BEDROCK", ""))

  private val testLayer: ZLayer[Client, Nothing, Bedrock] =
    Mantle.layer(MantleConfig(testApiKey, testRegion, testModelId))

  private def asTest(s: SharedSpec.BedrockScenario): Spec[Bedrock, Any] =
    test(s.name):
      (ZIO.sleep(500.millis) *> s.run).retry(
        Schedule.recurWhile[Any]:
          case _: (Error.Throttling | Error.InternalServer | Error.ServiceUnavailable | Error.ModelTimeout) => true
          case _ => false
        && Schedule.exponential(1.second)
        && Schedule.recurs(5),
      )

  def spec = suite("Bedrock Mantle Live Integration")(
    (SharedSpec.bedrockLiveScenarios
      ++ SharedSpec.bedrockRequestLiveScenarios
      ++ SharedSpec.bedrockLoopIntegrationScenarios
      ++ List(SharedSpec.textStreamScenario, SharedSpec.loopTextStreamScenario, SharedSpec.loopAsStreamScenario, SharedSpec.loopPrimitiveInputHandler)).map(asTest)*
  ).provideSomeShared[Scope](Client.default, testLayer)
    @@ ifEnvSet("AWS_BEARER_TOKEN_BEDROCK")
    @@ withLiveClock
    @@ withLiveSystem
    @@ timeout(90.seconds)
    @@ sequential
