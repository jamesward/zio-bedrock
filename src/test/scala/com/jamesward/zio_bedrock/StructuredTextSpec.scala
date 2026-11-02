package com.jamesward.zio_bedrock

import com.jamesward.zio_bedrock.internal.Helpers
import zio.json.ast.Json
import zio.schema.codec.json.schemaJson
import zio.test.*

/** Structured output a model wrote as text: the object is found whatever surrounds it. */
object StructuredTextSpec extends ZIOSpecDefault:
  private def obj(text: String) = Helpers.decodeStructured[Json](text).toOption.flatMap(_.asObject).map(_.toString)

  def spec = suite("structured output in text")(
    test("JSON followed by prose with braces of its own, strings with braces and escaped quotes, and stray text before") {
      assertTrue(
        obj("""{"command":"./gradlew test","timeout":200000}The tests ran {see above}.""").contains("""{"command":"./gradlew test","timeout":200000}"""),
        obj("""Here: {"a":"x } \" {","b":[1,{"c":2}]} done}""").contains("""{"a":"x } \" {","b":[1,{"c":2}]}"""),
        obj("""{"a":1}""").contains("""{"a":1}"""),
        obj("no json").isEmpty,
      )
    },
  )
