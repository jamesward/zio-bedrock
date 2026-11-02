package com.jamesward.zio_bedrock.internal

import com.jamesward.zio_bedrock.Bedrock
import com.jamesward.zio_bedrock.Bedrock.*
import zio.http.endpoint.openapi.{JsonSchema as JS}

private[zio_bedrock] object Helpers:

  def withStrictObjects(s: JS): JS =
    s match
      case o: JS.Object =>
        o.copy(
          properties           = o.properties.view.mapValues(withStrictObjects).toMap,
          additionalProperties = Left(false),
        )
      case a: JS.AnnotatedSchema => a.copy(schema = withStrictObjects(a.schema))
      case a: JS.AllOfSchema     => a.copy(allOf  = a.allOf .map(withStrictObjects))
      case a: JS.AnyOfSchema     => a.copy(anyOf  = a.anyOf .map(withStrictObjects))
      case a: JS.OneOfSchema     => a.copy(oneOf  = a.oneOf .map(withStrictObjects))
      case a: JS.ArrayType       => a.copy(items  = a.items .map(withStrictObjects))
      case other                  => other

  /** As [[withStrictObjects]], for a JSON Schema document given as JSON: every object schema without an
    * `additionalProperties` gets `additionalProperties: false`, which Bedrock structured output requires. */
  def withStrictObjects(schema: zio.json.ast.Json): zio.json.ast.Json =
    import zio.json.ast.Json
    schema match
      case Json.Obj(fields) =>
        val strict = Json.Obj(fields.map((key, value) => key -> withStrictObjects(value)))
        val isObject = strict.get("type").contains(Json.Str("object")) || strict.get("properties").isDefined
        if isObject && strict.get("additionalProperties").isEmpty then Json.Obj(strict.fields :+ ("additionalProperties" -> Json.Bool(false)))
        else strict
      case Json.Arr(values) => Json.Arr(values.map(withStrictObjects))
      case other            => other

  def fromWireToolUse(tu: Wire.ToolUseContent): ContentBlock.ToolUse =
    ContentBlock.ToolUse(
      toolUseId = tu.toolUseId,
      name      = tu.name,
      input     = new ToolInput(tu.input),
    )

  def fromWireToolResult(tr: Wire.ToolResultContent): ContentBlock.ToolResult =
    ContentBlock.ToolResult(
      toolUseId = tr.toolUseId,
      content   = tr.content.flatMap:
        case Wire.ToolResultBlock.Text(t) => List(ToolResultBlock.Text(t))
        case Wire.ToolResultBlock.Json(j) => List(ToolResultBlock.Json(new ToolInput(j)))
        case _                            => Nil,
      status    = tr.status match
        case None                                 => null
        case Some(Wire.ToolResultStatus.Success)  => ToolResultStatus.Success
        case Some(Wire.ToolResultStatus.Error)    => ToolResultStatus.Error
      ,
    )

  def toWireContentBlock(cb: ContentBlock): Wire.ContentBlock =
    cb match
      case ContentBlock.Text(t) => Wire.ContentBlock.Text(t)
      case ContentBlock.ToolUse(id, name, input) =>
        Wire.ContentBlock.ToolUse(Wire.ToolUseContent(
          toolUseId = id,
          name      = name,
          input     = input.raw,
        ))
      case ContentBlock.ToolResult(id, content, status) =>
        Wire.ContentBlock.ToolResult(Wire.ToolResultContent(
          toolUseId = id,
          content   = content.map:
            case ToolResultBlock.Text(t) => Wire.ToolResultBlock.Text(t)
            case ToolResultBlock.Json(v) => Wire.ToolResultBlock.Json(v.raw),
          status    = status match
            case null                       => None
            case ToolResultStatus.Success   => Some(Wire.ToolResultStatus.Success)
            case ToolResultStatus.Error     => Some(Wire.ToolResultStatus.Error)
          ,
        ))

  def structuredOutputConfig[T](using schema: zio.schema.Schema[T]): Wire.OutputConfig =
    val rawJsonSchema = JS.fromZSchema(
      schema,
      JS.SchemaRef(JS.SchemaSpec.JsonSchema, JS.SchemaStyle.Inline),
    )
    val outputJsonSchema = withStrictObjects(rawJsonSchema).toJson
    Wire.OutputConfig(Wire.TextFormat.JsonSchema(
      Wire.JsonSchemaStructure(Wire.JsonSchemaSpec(
        schema = outputJsonSchema,
        name   = "structured_output",
      )),
    ))

  /** Decode structured output exactly, with a narrow recovery path for models
    * that wrap an otherwise-valid JSON object/array in prose or markdown.
    */
  /** The index of the bracket closing the one at `start`, skipping string contents; none when it isn't closed. */
  private def balancedEnd(text: String, start: Int): Option[Int] =
    @scala.annotation.tailrec
    def scan(i: Int, depth: Int, inString: Boolean, escaped: Boolean): Option[Int] =
      if i >= text.length then None
      else
        val c = text.charAt(i)
        if inString then scan(i + 1, depth, inString = !(c == '"' && !escaped), escaped = !escaped && c == '\\')
        else if c == '"' then scan(i + 1, depth, inString = true, escaped = false)
        else if c == '{' || c == '[' then scan(i + 1, depth + 1, inString = false, escaped = false)
        else if c == '}' || c == ']' then
          if depth == 1 then Some(i) else scan(i + 1, depth - 1, inString = false, escaped = false)
        else scan(i + 1, depth, inString = false, escaped = false)
    scan(start, 0, inString = false, escaped = false)

  def decodeStructured[T: zio.schema.Schema](text: String): Either[String, T] =
    val codec = zio.schema.codec.JsonCodec.schemaBasedBinaryCodec[T](Codecs.codecConfig)
    def decode(value: String): Either[String, T] =
      codec.decode(zio.Chunk.fromArray(value.getBytes(java.nio.charset.StandardCharsets.UTF_8)))
        .left.map(_.message)

    // Each `{` or `[` is a candidate start, ending at the last matching close: models sometimes put stray characters,
    // even a lone brace, before the JSON (`{\n{"a":1}`, `{"{"a":1}`), so the first candidate isn't always the one.
    decode(text).orElse:
      val starts = text.indices.filter(i => text.charAt(i) == '{' || text.charAt(i) == '[')
      if starts.isEmpty then Left("response did not contain a JSON object or array")
      else
        // each start tried to its balanced close first (JSON followed by prose: `{"a":1} Done, {see above}`), then
        // to the last close of its kind
        val attempts = starts.iterator.map: index =>
          val balanced = balancedEnd(text, index).map(end => decode(text.substring(index, end + 1)))
          balanced.filter(_.isRight).getOrElse:
            val end = if text.charAt(index) == '{' then text.lastIndexOf('}') else text.lastIndexOf(']')
            if end <= index then Left("response contained incomplete JSON") else decode(text.substring(index, end + 1))
        val first = attempts.next()
        if first.isRight then first else attempts.find(_.isRight).getOrElse(first)
