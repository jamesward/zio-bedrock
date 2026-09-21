package com.jamesward.zio_bedrock

import zio.*
import zio.http.Client
import zio.test.*

object MantleConfigSpec extends ZIOSpecDefault:

  private val configured = Client.default >>> Mantle.configured

  private def clearConfig: UIO[Unit] =
    ZIO.foreachDiscard(
      List("AWS_BEARER_TOKEN_BEDROCK", "BEDROCK_MODEL_ID", "AWS_REGION"),
    )(name => TestSystem.clearEnv(name))

  def spec = suite("Mantle configuration")(
    test("configuration rendering redacts the API key") {
      val rendered = MantleConfig(
        Bedrock.ApiKey("super-secret-mantle-key"),
        Bedrock.Region.UsEast1,
        Bedrock.ModelId("test-model"),
      ).toString
      assertTrue(rendered.contains("<redacted>"), !rendered.contains("super-secret-mantle-key"))
    },
    test("configured rejects an invalid explicit region") {
      for
        _ <- clearConfig
        _ <- TestSystem.putEnv("AWS_BEARER_TOKEN_BEDROCK", "test-key")
        _ <- TestSystem.putEnv("BEDROCK_MODEL_ID", "test-model")
        _ <- TestSystem.putEnv("AWS_REGION", "not-a-region")
        exit <- ZIO.service[Bedrock].provideLayer(configured).exit
      yield assertTrue(exit.causeOption.exists(_.failureOption.contains(MantleConfigError.InvalidRegion("not-a-region"))))
    },
    test("configured accepts AWS_BEARER_TOKEN_BEDROCK and defaults the region") {
      for
        _ <- clearConfig
        _ <- TestSystem.putEnv("AWS_BEARER_TOKEN_BEDROCK", "test-key")
        _ <- TestSystem.putEnv("BEDROCK_MODEL_ID", "test-model")
        service <- ZIO.service[Bedrock].provideLayer(configured)
      yield assertTrue(service != null)
    },
    test("configured reports a missing API key") {
      for
        _ <- clearConfig
        _ <- TestSystem.putEnv("BEDROCK_MODEL_ID", "test-model")
        exit <- ZIO.service[Bedrock].provideLayer(configured).exit
      yield assertTrue(exit.causeOption.exists(_.failureOption.contains(MantleConfigError.MissingApiKey)))
    },
    test("configured reports a missing model ID") {
      for
        _ <- clearConfig
        _ <- TestSystem.putEnv("AWS_BEARER_TOKEN_BEDROCK", "test-key")
        exit <- ZIO.service[Bedrock].provideLayer(configured).exit
      yield assertTrue(exit.causeOption.exists(_.failureOption.contains(MantleConfigError.MissingModelId)))
    },
  )
