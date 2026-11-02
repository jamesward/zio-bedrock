package com.jamesward.zio_bedrock

import com.jamesward.zio_bedrock.Bedrock.*
import com.jamesward.zio_bedrock.internal.{Codecs, ConverseWire, MantleWire, Tools}
import zio.json.*
import zio.json.ast.Json
import zio.schema.codec.JsonCodec
import zio.test.*

/** A reasoning effort reaches each API where it takes it, and nothing is sent without one. */
object ReasoningEffortSpec extends ZIOSpecDefault:
  private def encoded(config: InferenceConfig) =
    val wire = Tools.toWire(RequestConfig(messages = List(Message.user("hi")), inferenceConfig = config), None)
    val converse = JsonCodec.schemaBasedBinaryCodec[ConverseWire.Request](Codecs.codecConfig).encode(ConverseWire.request(wire))
    val mantle = JsonCodec.schemaBasedBinaryCodec[MantleWire.Request](Codecs.codecConfig).encode(MantleWire.request(wire, ModelId("m"), streaming = false))
    (new String(converse.toArray, "UTF-8").fromJson[Json].toOption.get, new String(mantle.toArray, "UTF-8").fromJson[Json].toOption.get)

  private def at(json: Json, path: String*) = path.foldLeft(Option(json))((j, k) => j.flatMap(_.asObject).flatMap(_.get(k)))

  def spec = suite("reasoning effort")(
    test("Mantle's reasoning_effort, Converse's additionalModelRequestFields, the rest of inferenceConfig as before") {
      val (converse, mantle) = encoded(InferenceConfig(maxTokens = 100, temperature = 0.0, reasoningEffort = "low"))
      assertTrue(
        at(mantle, "reasoning_effort").contains(Json.Str("low")),
        at(converse, "additionalModelRequestFields", "reasoning_effort").contains(Json.Str("low")),
        at(converse, "inferenceConfig", "maxTokens").contains(Json.Num(100)),
        at(converse, "inferenceConfig", "reasoningEffort").isEmpty,
      )
    },
    test("unset, neither API is sent one") {
      val (converse, mantle) = encoded(InferenceConfig(maxTokens = 100))
      assertTrue(at(mantle, "reasoning_effort").forall(_.asNull.isDefined), at(converse, "additionalModelRequestFields").forall(_.asNull.isDefined),
        at(converse, "inferenceConfig", "temperature").forall(_.asNull.isDefined))
    },
  )
